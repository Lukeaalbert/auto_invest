from typing import Set, List
from pathlib import Path
import string
import csv

class AssetFetcher:
    def __parse_channels(self) -> List[str]:
        """
        Internal helper function.
        Parses the file of youtubers to fetch and returns a list of them 
        (in order of priority).
        """
        channels = []
        with open(self.__filename, 'r') as f:
            next(f) # skip header
            for line in f:
                name, priority = line.strip().split(',')
                channels.append((name.strip(), int(priority.strip())))
        
        # sort based on prioritys
        channels.sort(key=lambda x: x[1], reverse=True)

        return [name for name, _ in channels]

    def __fetch_assets(self) -> Set[str]:
        """
        Main logic. fetches assets using user defined hueristic/fetching method.
        """ 
        yt_channels = self.__parse_channels()

    def __init__(self, filename: string, maxAssets: int = 5):
        self.__maxAssets = maxAssets
        self.__filename = filename
        self.__fetch_assets()

if __name__ == "__main__":
    filename = Path.cwd().parent.parent / 'files' / 'data_in' / 'source_youtubers.csv'
    asset_fetcher = AssetFetcher(filename, 5)