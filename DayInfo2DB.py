import pymysql, time
from pymysql.cursors import DictCursor
from pykrx import stock
from DayInfo import DayInfoCrawler
import pandas as pd

# 하루씩 저장 
# 60번 해야할 듯 
def main():

    # Config your MySQL Setting
    conn = pymysql.connect(user='root', port=3307, passwd = ' ', host = 'localhost', db = 'stock_db', charset= 'utf8')
    cursor = conn.cursor(DictCursor)

    sc = DayInfoCrawler();   sc.GetWeekDays()
    

    for day in sc.days:
        ohlcv_1 = stock.get_market_ohlcv_by_ticker(day, market="KOSPI"); time.sleep(1)
        ohlcv_2 = stock.get_market_ohlcv_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        fundamental_1 = stock.get_market_fundamental_by_ticker(day, market="KOSPI")[['BPS','PER','PBR','EPS']]; time.sleep(1)
        fundamental_2 = stock.get_market_fundamental_by_ticker(day, market="KOSDAQ")[['BPS','PER','PBR','EPS']]; time.sleep(1)
        marketcap_1 = stock.get_market_cap_by_ticker(day, market="KOSPI")[['시가총액']]; time.sleep(1)
        marketcap_2 = stock.get_market_cap_by_ticker(day, market="KOSDAQ")[['시가총액']]; time.sleep(1)
        
        ohlcv = pd.concat([ohlcv_1, ohlcv_2], axis=0, join='inner')
        fundamental = pd.concat([fundamental_1, fundamental_2], axis=0, join='inner')
        marketcap = pd.concat([marketcap_1, marketcap_2], axis=0, join='inner')

        one_day_info = pd.concat([ohlcv, fundamental, marketcap], axis=1, join='inner').to_dict('index')
    
    for key, value in list(one_day_info.items()):
        try:
            sql = (
                    "INSERT INTO DAY_INFO(stock_id, date, open_price, high_price, low_price, closing_price," 
                    "transaction_value,transaction_amount, fluctuation_rate, BPS, PER, PBR, EPS, market_cap)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )

            cursor.execute(sql, (key, '2021-11-25', value['시가'], value['고가'], value['저가'], value['종가'], value['거래량'], int((value['거래대금'])//1000000),
                                round(value['등락률'],3) , value['BPS'], value['PER'], value['PBR'], value['EPS'], int((value['시가총액']//100000000))))
        except:
            pass

    cursor.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()