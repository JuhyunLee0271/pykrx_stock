import pymysql, time
from pymysql.cursors import DictCursor
from pykrx import stock

def main():

    # DB Connection
    conn = pymysql.connect(user='root', port=3307, passwd = ' ', host = 'localhost', db = 'stock_db', charset= 'utf8')
    cursor = conn.cursor(DictCursor)

    ticker = stock.get_market_ticker_list(market="KOSPI") + stock.get_market_ticker_list(market="KOSDAQ"); time.sleep(1)
    stock_dict = {}
    for code in ticker:
        name = stock.get_market_ticker_name(code)
        stock_dict[code] = name
    
    for key, value in list(stock_dict.items()):
        sql = "INSERT INTO STOCK(Stock_id, Name) VALUES (%s, %s)"
        cursor.execute(sql, (key, value))
    
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()