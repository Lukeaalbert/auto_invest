from typing import List
from pathlib import Path
import yfinance as yf
from datetime import datetime, timedelta

class AssetPurchaser:
    def __get_stock_price(self, ticker_symbol):
        stock = yf.Ticker(ticker_symbol)
        hist = stock.history(period="1d")
        if not hist.empty:
            current_price = hist['Close'].iloc[-1]
            return float(current_price)
        else:
            return -1.0
    
    def __purchase_asset(self, asset: str, purchase_quantity: float,
        expiration_date: str):
        """
        Makes Public API call to purchase purchase_quantity of an asset 
        """
        # simulate purchase
        if (self.__simulation_mode):
            # get ticker and make sure it's valid
            ticker_price = self.__get_stock_price(asset)
            if (ticker_price == -1.0):
                print(f"Asset Purchaser Error: unable to retrieve price for ticker {asset}")
            # simulation purchase
            self.file.write(f"{asset}, {ticker_price}, {purchase_quantity}, {expiration_date}\n")
        return

    def __purchase_assets(self):
        """
        Purchases the specified amount of each asset.
        """
        # get expiration date as string
        expiration_date_str = (datetime.now() + timedelta(days=self.__valid_purchase_days)).strftime('%Y/%m/%d')
        for asset, purchase_quantity in zip(self.__assets, self.__asset_purchase_amounts):
            self.__purchase_asset(asset, purchase_quantity, expiration_date_str)
    
    def __init__(self, assets: List[str],  valid_purchase_days: int,
        asset_purchase_amounts: List[float] = None, 
        universal_purchase_amount: float = None,
        simulation_mode: bool = True):
        if (asset_purchase_amounts == None):
            # user must specify either asset_purchase_amounts or universal_purchase_amount
            if (universal_purchase_amount == None):
                print("Asset Purchaser Error: either asset_purchase_amounts or universal_purchase_amount field must be specified.")
                return
            # dynamically create asset_purchase_amounts if not specified by user
            asset_purchase_amounts = [universal_purchase_amount] * len(assets)
        # set class variables
        self.__assets = assets
        self.__asset_purchase_amounts = asset_purchase_amounts
        self.__simulation_mode = simulation_mode
        self.__valid_purchase_days = valid_purchase_days
        # open sim file if in simulation mode
        if (simulation_mode):
            portfolio_simulation_file = Path.cwd().parent.parent / 'files' / 'data_out' / 'portfolio_simulation.csv'
            self.file = open(portfolio_simulation_file, 'a')
        # purchase assets
        self._AssetPurchaser__purchase_assets()


if __name__ == "__main__":
    assets = ["AAPL", "MU", "TSM"]
    # construct asset purchaser
    asset_purchaser = AssetPurchaser(assets=assets,
        valid_purchase_days=4,
        universal_purchase_amount=1000.0)