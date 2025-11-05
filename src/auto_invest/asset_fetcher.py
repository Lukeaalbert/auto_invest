from typing import Set, List, Any
from pathlib import Path
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter
import google.generativeai as genai
import googleapiclient.discovery
from collections import Counter
import re
import csv
import os
import json
import datetime

# load environment variables from .env file
load_dotenv()
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
gemini_api_key = os.getenv("GEMINI_API_KEY")

# configure the Google AI SDK
genai.configure(api_key=gemini_api_key)

class AssetFetcher:
    def __parse_channels(self) -> List[Set[str]]:
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

    def __get_new_videos_from_channels(self, channel_list) -> List[str]:
        """
        retrieves newly uploaded videos from all youtube channels in channel_list from the
        last self.__days_ago days

        returns:
            list: a list of new video_ids, sorted by priority.
        """
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=youtube_api_key)

        # retrieve videos from the uploads playlist.
        # ordered by channel and publish date.
        # i.e., (channel_1_vid_1, channel_1_vid_2, ..., channel_2_vid_1, channel_2_vid_2)
        videos = []

        for channel in channel_list:
            # get channel id
            channel_id = channel[1]

            # calculate the publish date threshold
            published_after_date = (datetime.datetime.now() - datetime.timedelta(days=self.__days_ago)).isoformat("T") + "Z"

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
                    maxResults=50, # max allowed results per page
                    pageToken=next_page_token
                ).execute()

                for item in playlist_items_response["items"]:
                    video_publish_date_str = item["snippet"]["publishedAt"]
                    video_publish_date = datetime.datetime.fromisoformat(video_publish_date_str.replace("Z", "+00:00"))

                    if video_publish_date >= datetime.datetime.fromisoformat(published_after_date.replace("Z", "+00:00")):
                        videos.append(item["snippet"]["resourceId"]["videoId"])
                    else:
                        # videos are ordered by publish date (newest first), so if we hit an older video, we can stop
                        next_page_token = None
                        break # exit the inner loop and stop fetching more pages

                next_page_token = playlist_items_response.get("nextPageToken")
                if not next_page_token:
                    break

        return videos
    
    def __get_youtube_transcripts(self, video_ids) -> List[str]:
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

    def __extract_recommendations_from_transcript(self, transcript: str,
        model_object: genai.GenerativeModel) -> List[str]:
        """
        use Gemini to extract a list of stocks recommended in the transcript.
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

    def __aggregate_recommendations(self, transcripts: List[str]) -> List[str]:
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
                stocks = self.__extract_recommendations_from_transcript(t, model)
            except Exception as e:
                print(f"Warning: error processing transcript: {e}")
                stocks = []
            all_stocks.extend(stocks)
            
        # count frequencies
        counts = Counter(all_stocks)
        # sort stocks by descending frequency
        sorted_stocks = [stock for stock, _ in counts.most_common()]
        return sorted_stocks

    def __fetch_assets(self) -> Set[str]:
        """
        Main logic. fetches assets using user defined hueristic/fetching method.
        """ 
        yt_channels = self.__parse_channels()
        new_videos = self.__get_new_videos_from_channels(yt_channels)
        transcripts = self.__get_youtube_transcripts(new_videos)
        reccomendations = self.__aggregate_recommendations(transcripts)
        for rec in reccomendations:
            print(rec)

    def __init__(self, channels_filename: str, days_ago: int = 1):
        self.__channels_filename = channels_filename
        self.__days_ago = days_ago
        self.__fetch_assets()

if __name__ == "__main__":
    # file paths
    channels_filename = Path.cwd().parent.parent / 'files' / 'data_in' / 'source_youtubers.csv'

    # construct asset fetcher
    asset_fetcher = AssetFetcher(channels_filename=channels_filename, days_ago=5)