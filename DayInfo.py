"""
pykrx: 한국거래소 주식 정보 조회 API

get_market_ohlcv_by_ticker: 날짜별 시가, 고가, 저가, 종가, 거래량 
get_market_fundamental_by_ticker: 날짜별 DIV, BPS, PER, EPS, PBR 
get_market_cap_by_ticker: 날짜별 시가총액, 거래량, 거래대금, 상장주식수

(pending)
get_market_trading_value_by_ticker: 날짜별 기관합계, 기타법인, 개인, 외국인합계 (거래대금)
get_market_trading_volume_by_ticker: 날짜별 기관합계, 기타법인, 개인, 외국인합계 (거래량)

"""
from pykrx import stock
import pandas as pd
import time
from datetime import datetime, timedelta

# Class For "DAY_INFO"
class DayInfoCrawler:
    # KOSPI = 939, KOSDAQ = 1528
    # 총 2467개의 종목, 2467 * 60 = 148020 tuples
    def __init__(self, interval):
        self.CODE = stock.get_market_ticker_list(market="KOSPI") + stock.get_market_ticker_list(market="KOSDAQ")
        self.interval = interval
        self.today = datetime.now().date()
        self.days = []

    # 휴일을 제외한 interval days
    # sc.days
    # ['20211108', '20211109', '20211110', ...]
    def GetWeekDays(self):
        startDate = self.today
        while len(self.days) < self.interval:
            if 0 <= startDate.weekday() <= 4:
                weekday = str(startDate).replace('-','')
                self.days.append(weekday)
            startDate -= timedelta(1)
        self.days.reverse()     

def main():
    # 종목 코드 정보: sc.CODE
    DAY_INFO = dict()

    # Set interval 
    interval = 10
    sc = DayInfoCrawler(interval);  sc.GetWeekDays()
    
    # 시가, 고가, 저가, 종가, 거래량, 거래대금, 등락률, BPS, PER, PBR, EPS, DIV, DPS, 종가, 시가총액, 거래량, 거래대금, 상장주식수
    for day in sc.days:
        ohlcv_1 = stock.get_market_ohlcv_by_ticker(day, market="KOSPI"); time.sleep(1)
        ohlcv_2 = stock.get_market_ohlcv_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        fundamental_1 = stock.get_market_fundamental_by_ticker(day, market="KOSPI"); time.sleep(1)
        fundamental_2 = stock.get_market_fundamental_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        marketcap_1 = stock.get_market_cap_by_ticker(day, market="KOSPI"); time.sleep(1)
        marketcap_2 = stock.get_market_cap_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        
        ohlcv = pd.concat([ohlcv_1, ohlcv_2], axis=0, join='inner')
        fundamental = pd.concat([fundamental_1, fundamental_2], axis=0, join='inner')
        marketcap = pd.concat([marketcap_1, marketcap_2], axis=0, join='inner')
        
        one_day_info = pd.concat([ohlcv, fundamental, marketcap], axis=1, join='inner').to_dict('index')
        DAY_INFO[day] = one_day_info

if __name__ == "__main__":
    main()
