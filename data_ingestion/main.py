import requests
import psycopg2
import time
import sys

DB_CONFIG = {
    "host": "db",
    "database": "crypto_db",
    "user": "user",
    "password": "password",
    "port": "5432"
}

def fetch_btc_price():
    """Отримує актуальну ціну BTC з Binance API"""
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        data = response.json()
        return data['symbol'], float(data['price'])
    except Exception as e:
        print(f"❌ Помилка API: {e}")
        return None, None

def save_to_db(symbol, price):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO bitcoin_prices (symbol, price) VALUES (%s, %s)",
            (symbol, price)
        )
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Збережено в БД: {symbol} -> {price}")
    except Exception as e:
        print(f"❌ Помилка БД: {e}")

if __name__ == "__main__":
    print("🚀 Запуск стримінгу даних...")
    while True:
        try:
            symbol, price = fetch_btc_price()
            if symbol and price:
                save_to_db(symbol, price)
            print("💤 Очікування 60 секунд до наступного оновлення...")
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n🛑 Стрімінг зупинено користувачем.")
            break
        except Exception as e:
            print(f"⚠️ Непередбачена помилка в циклі: {e}")
            time.sleep(10)