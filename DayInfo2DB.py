import pymysql, time
from pymysql.cursors import DictCursor
from pykrx import stock
from DayInfo import DayInfoCrawler
import pandas as pd

def main():
    conn = pymysql.connect(user='root', port=3307, passwd = ' ', host = 'localhost', db = 'stock_db', charset= 'utf8')
    cursor = conn.cursor(DictCursor)

    sc = DayInfoCrawler();   sc.GetWeekDays()

    for day in sc.days:
        ohlcv_1 = stock.get_market_ohlcv_by_ticker(day, market="KOSPI"); time.sleep(1)
        ohlcv_2 = stock.get_market_ohlcv_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        fundamental_1 = stock.get_market_fundamental_by_ticker(day, market="KOSPI"); time.sleep(1)
        fundamental_2 = stock.get_market_fundamental_by_ticker(day, market="KOSDAQ"); time.sleep(1)
        marketcap_1 = stock.get_market_cap_by_ticker(day, market="KOSPI")[['시가총액']]; time.sleep(1)
        marketcap_2 = stock.get_market_cap_by_ticker(day, market="KOSDAQ")[['시가총액']]; time.sleep(1)
        
        ohlcv = pd.concat([ohlcv_1, ohlcv_2], axis=0, join='inner')
        fundamental = pd.concat([fundamental_1, fundamental_2], axis=0, join='inner')
        marketcap = pd.concat([marketcap_1, marketcap_2], axis=0, join='inner')

        one_day_info = pd.concat([ohlcv, fundamental, marketcap], axis=1, join='inner').to_dict('index')
    
    for key, value in list(one_day_info.items())[:3]:
        sql = ("INSERT INTO DAY_INFO(stock_id, date, open_price, high_price, low_price, closing_price," 
                "transaction_value,transaction_amount, BPS, PER, PBR, EPS, DIV, DPS, market_cap)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        cursor.execute(sql, (key, value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8], value[9], value[10], value[11], value[12], value[13], value[14]))
    
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()