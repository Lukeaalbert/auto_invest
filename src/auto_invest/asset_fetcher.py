from typing import Set, List, Any
from pathlib import Path
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter
import google.generativeai as genai
from googleapiclient.discovery import build
from collections import Counter
from datetime import datetime, timedelta, timezone
import re
import csv
import os
import json

# load environment variables from .env file
load_dotenv()
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
gemini_api_key = os.getenv("GEMINI_API_KEY")

# configure the Google AI SDK
genai.configure(api_key=gemini_api_key)

class AssetFetcher:

    '''
    Fetching Asset Logic
    '''

    def openVideoIdCache(self, mode: str):
        valid_modes = {"a", "w", "r"}

        video_id_cache_path = (
            Path.cwd().parent.parent / "files" / "data_out" / "video_id_cache.csv"
        )

        self.file = open(video_id_cache_path, mode, encoding="utf-8")
        return self.file

    def parseChannelFile(self) -> List[Set[str]]:
        """
        Internal helper function.
        Parses the file of youtubers to fetch and returns a list of them 
        (in order of priority).
        """
        channels = []
        with open(self.__channels_filename, 'r') as f:
            next(f) # skip header
            for line in f:
                name, channel_id, priority = line.strip().split(',')
                channels.append((name.strip(), channel_id.strip(), int(priority.strip())))
        
        # sort based on prioritys
        channels.sort(key=lambda x: x[2], reverse=True)

        return [(name, channel_id) for name, channel_id, _ in channels]

    def getVideoIdsFromChannels(self, channel_list) -> List[str]:
        """
        retrieves newly uploaded videos from all YouTube channels in `channel_list`
        within the last `self.__delta_video_days` days.
        Note that if in simulation mode, this is relative to `self.__simulation_run_date`
        (defaults to today).
        
        returns a list of new video IDs, sorted by priority.
        """

        youtube = build("youtube", "v3", developerKey=youtube_api_key)

        # use provided date or now
        if ((not self.__simulation_mode) or (self.__simulation_run_date is None)):
            self.__simulation_run_date = datetime.utcnow()

        # calculate the publish date threshold
        if self.__simulation_run_date is None:
            self.__simulation_run_date = datetime.now(timezone.utc)
        elif self.__simulation_run_date.tzinfo is None:
            self.__simulation_run_date = self.__simulation_run_date.replace(tzinfo=timezone.utc)

        published_after_date = (self.__simulation_run_date - timedelta(days=self.__delta_video_days)).isoformat("T")

        videos = []

        for channel in channel_list:
            channel_id = channel[1]

            # get the uploads playlist ID for the channel
            channel_response = youtube.channels().list(
                part="contentDetails",
                id=channel_id
            ).execute()

            uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            next_page_token = None

            while True:
                playlist_items_response = youtube.playlistItems().list(
                    part="snippet",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()

                for item in playlist_items_response["items"]:
                    video_publish_date_str = item["snippet"]["publishedAt"]
                    video_publish_date = datetime.fromisoformat(video_publish_date_str.replace("Z", "+00:00"))
                    threshold_date = datetime.fromisoformat(published_after_date.replace("Z", "+00:00"))

                    # check if video is within the date window
                    threshold_date = datetime.fromisoformat(published_after_date.replace("Z", "+00:00"))
                    if threshold_date <= video_publish_date <= self.__simulation_run_date:
                        videos.append(item["snippet"]["resourceId"]["videoId"])
                    elif video_publish_date < threshold_date:
                        # since videos are ordered by newest → oldest, stop once we pass threshold
                        next_page_token = None
                        break

                next_page_token = playlist_items_response.get("nextPageToken")
                if not next_page_token:
                    break

        return videos
    
    def getTranscriptsFromVideoIds(self, video_ids) -> List[str]:
        """
        retrieves the transcripts for the YouTube videos specified in video_ids.
    
        returns:
            a list of the transcripts as json strings
        """
        # list of transcripts
        transcripts = []
        ytt_api = YouTubeTranscriptApi()

        for video_id in video_ids:
            try:
                # get the transcript
                transcript = ytt_api.fetch(video_id)

                # turn transcript into JSON string
                formatter = JSONFormatter()
                json_formatted = formatter.format_transcript(transcript)

                # append to transcripts list
                transcripts.append(json_formatted)
            except Exception as e:
                print(f"Error retrieving transcript for video ID {video_id}: {e}")

        return transcripts

    def extractRecommendationsFromTranscript(self, transcript: str,
        model_object: genai.GenerativeModel) -> List[str]:
        """
        use gemini to extract a list of stocks recommended in the transcript.
        returns a list of stock tickers or names found in that transcript.
        """
        user_prompt_template = (
            "Analyze the following YouTube video transcript. "
            "Extract every stock ticker that the creator **explicitly recommends buying or strongly endorses**. "
            "Do not include stocks that are only mentioned, criticized, or used as examples. "
            "Only list each recommended stock once. "
            "Output the result as a single JSON object adhering strictly to the required format.\n\n"
            "Transcript:\n\n{transcript}"
        )
        
        prompt = user_prompt_template.format(transcript=transcript)
        
        response = model_object.generate_content(
            contents=[{"parts":[{"text": prompt}]}]
        )

        resp_text = response.text.strip()
        
        try:
            resp_text_cleaned = re.sub(r'^\s*```(?:json)?\s*|```\s*$', '', resp_text, flags=re.DOTALL | re.IGNORECASE).strip()
            # attempt to find and load the JSON object (in case of minor preamble/postamble)
            json_match = re.search(r"\{.*\}", resp_text_cleaned, re.DOTALL)
            if json_match:
                resp_text_final = json_match.group(0)
            else:
                resp_text_final = resp_text_cleaned
            resp_json = json.loads(resp_text_final)
            stocks = resp_json.get("recommended_stocks", [])
            if not isinstance(stocks, list):
                stocks = []
        except Exception as e:
            print(f"Warning: Failed to parse JSON from Gemini. Falling back to regex. Error: {e}")
            print(f"Response: {resp_text}")
            
        return stocks

    def identifyAssetsFromTranscript(self, transcripts: List[str]) -> List[str]:
        """
        parses multiple transcripts and returns a sorted list of unique stocks,
        roughly sorted by how many times they were recommended across all transcripts.
        """
        # thorough system instructions
        system_instruction = (
            "You are an expert financial analyst and transcription parsing tool. "
            "Your sole task is to analyze a given video transcript and extract a list of "
            "explicitly recommended stocks. A recommendation means the creator "
            "uses terms like 'buy,' 'invest in,' 'I own this,' 'strong long-term potential,' "
            "or 'this is a stock I'd pick.' "
            "You **must** only output a single JSON object. "
            "The JSON object **must** have a field named 'recommended_stocks' which is a list of strings. "
            "Each string in the list must be a stock ticker. "
            "Do not include any text, notes, or explanations outside of the JSON object. "
            "If no stocks are explicitly recommended, the list should be empty: { \"recommended_stocks\": [] }."
        )

        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=system_instruction
        )
        
        all_stocks = []
        for t in transcripts:
            try:
                # pass the configured model object
                stocks = self.extractRecommendationsFromTranscript(t, model)
            except Exception as e:
                print(f"Warning: error processing transcript: {e}")
                stocks = []
            all_stocks.extend(stocks)
            
        # count frequencies
        counts = Counter(all_stocks)
        # sort stocks by descending frequency
        sorted_stocks = [stock for stock, _ in counts.most_common()]
        return sorted_stocks

    def fetchAssets(self) -> Set[str]:
        """
        main logic. fetches assets using user defined hueristic/fetching method.
        """ 
        yt_channels = self.parseChannelFile()
        video_ids = self.getVideoIdsFromChannels(yt_channels)
        # Note: TODO
        # new_video_ids = FilterOutSeenVideoIds(yt_channels)
        # self.__new_youtube_video_ids = new_video_ids
        self.__new_youtube_video_ids = video_ids # TODO delete me
        transcripts = self.getTranscriptsFromVideoIds(video_ids)
        reccomendations = self.identifyAssetsFromTranscript(transcripts)
        self.__fetched_recommendations = reccomendations
    
    '''
    Getters
    '''
    
    def getFetchedAssets(self):
        return self.__fetched_recommendations
    
    def getNewYoutubeVideoIds(self):
        return self.__new_youtube_video_ids

    '''
    Constructor
    '''

    def __init__(self, channels_filename: str, delta_video_days: int,
        simulation_mode: bool = True, simulation_run_date: datetime = None):
        # set simulation specific member variables, if needed
        if (simulation_mode):
            if (simulation_run_date == None):
                print(f"Asset Fetcher Error: must provide a simulation run date when in simulation mode.")
            else:
                self.__simulation_run_date = simulation_run_date
        self.__simulation_mode = simulation_mode
        self.__channels_filename = channels_filename
        self.__delta_video_days = delta_video_days
        # invoke asset fetching
        self.fetchAssets()

if __name__ == "__main__":
    # file paths
    channels_filename = Path.cwd().parent.parent / 'files' / 'data_in' / 'source_youtubers.csv'

    # date
    custom = datetime(2025, 1, 1)

    # construct asset fetcher
    asset_fetcher = AssetFetcher(channels_filename=channels_filename,
        delta_video_days=5, simulation_mode=True, simulation_run_date=custom)
    
    fetched_assets = asset_fetcher.getFetchedAssets()

    for asset in fetched_assets:
        print(asset)
    
    video_ids = asset_fetcher.getNewYoutubeVideoIds()

    for video in video_ids:
        print(video)

    # TODO:
    # impliment FilterOutSeenVideoIds method and video ID cache