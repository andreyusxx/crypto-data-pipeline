import requests
import psycopg2
import sys
from config import DB_CONFIG, SYMBOLS


def fetch_crypto_prices(symbols):
    symbols_param = str(symbols).replace(" ", "").replace("'", '"')
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbols={symbols_param}"
    response = requests.get(url, timeout=10)
    response.raise_for_status() 
    return response.json()


def save_to_db(symbol, price, volume, event_time):
    table_name = f"prices_{symbol.replace('USDT', '').lower()}"
        
    query = f"""
        INSERT INTO {table_name} (price, volume, event_time)
        VALUES (%s, %s, %s)
    """
        
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (price, volume, event_time))
            conn.commit()

def check_db_connection():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            return True
    except Exception:
        return False

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "stream"
    if not check_db_connection():
        exit(1)

    if mode == "once": 
        prices_data = fetch_crypto_prices(SYMBOLS)
        if prices_data:
            for data in prices_data:
                save_to_db(data['symbol'], float(data['lastPrice']), float(data['volume']), data['closeTime'])
        exit(0)