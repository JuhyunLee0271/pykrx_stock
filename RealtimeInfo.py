from pykrx import stock
import time, schedule
from datetime import datetime

"""
RealtimeInfo 
    KOSPI, KOSDAQ의 총 2467개의 종목들에 대해 매 10분마다 실시간(종가) 가격, 등락률, 거래량, 거래대금을 거래대금으로 정렬해 price_now 딕셔너리에 저장
"""

# Class For "STOCK"
class TodayInfoCrawler:
    def __init__(self):
        self.CODE = stock.get_market_ticker_list(market="KOSPI") + stock.get_market_ticker_list(market="KOSDAQ")
        self.today = str(datetime.now().date()).replace('-','')
        self.KOSPI = None
        self.KOSDAQ = None
        self.PRICE_NOW = None
    
    # 거래대금 별로 정렬
    def UpdateStockPrice(self):
        self.KOSPI = stock.get_market_ohlcv_by_ticker(self.today, market="KOSPI")[['등락률', '종가', '거래량', '거래대금']].to_dict('index')
        time.sleep(1)
        self.KOSDAQ = stock.get_market_ohlcv_by_ticker(self.today, market="KOSDAQ")[['등락률', '종가', '거래량', '거래대금']].to_dict('index')
        self.PRICE_NOW = dict(sorted(dict(self.KOSPI, **self.KOSDAQ).items(), key=lambda x: -x[1]['거래대금']))


# def sched(sc):
#     sc.UpdateStockPrice()

def main():
    # 종목 코드 정보: sc.CODE
    sc = TodayInfoCrawler(); sc.UpdateStockPrice()
    # sc.PRICE_NOW
    # 종목별 등락률, 종가, 거래량, 거래대금이 딕셔너리로 
    

    # time_quantum = 10
    # schedule.every(time_quantum).minutes.do(sched, sc)

    # Update Stock Price every 10 minutes
    # while True:
        # schedule.run_pending()
        # time.sleep(1)
        # print(sc.PRICE_NOW)

if __name__ == "__main__":
    main()
        
        