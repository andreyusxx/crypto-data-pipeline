import requests
import psycopg2
import time
import sys
import logging
import time

DB_CONFIG = {
    "host": "db",
    "database": "crypto_db",
    "user": "user",
    "password": "password",
    "port": "5432"
}
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ],
    force=True
)

def fetch_crypto_prices(symbols):
    symbols_param = str(symbols).replace(" ", "").replace("'", '"')
    url = f"https://api.binance.com/api/v3/ticker/24hr?symbols={symbols_param}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        return response.json()
    except Exception as e:
        logging.info(f"❌ Помилка API: {e}")
        return []

def save_to_db(symbol, price,volume):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO bitcoin_prices (symbol, price, volume) VALUES (%s, %s, %s)",
            (symbol, price, volume)
        )
        
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"✅ Збережено в БД: {symbol} -> {price}, Об'єм: {volume}")
    except Exception as e:
        logging.info(f"❌ Помилка БД: {e}")

if __name__ == "__main__":
    logging.info("🚀 Запуск стримінгу даних...")
    last_prices = {}

    while True:
        try:
            prices_data = fetch_crypto_prices(SYMBOLS)

            if not prices_data:
                logging.warning("⚠️ Дані від API не отримані.")

            for data in prices_data:
                symbol = data['symbol']
                current_price = float(data['lastPrice'])
                current_volume = float(data['volume'])

                if symbol in last_prices:
                    prew_price = last_prices[symbol]
                    diff = current_price - prew_price
                    change_percent = abs(current_price - prew_price) / prew_price * 100
                    trend = "📈" if diff > 0 else "📉" if diff < 0 else "↔️"

                    
                    if change_percent > 50:
                        logging.warning(f"⚠️ АНОМАЛІЯ: Ціна змінилася на {change_percent:.2f}%. Запис ігноровано. Поточна: {current_price}, Попередня: {last_price}")
                        time.sleep(60)
                        continue
                    logging.info(f"Аналіз [{symbol}]: {trend} Зміна: {change_percent:.4f}%")

                save_to_db(symbol, current_price, current_volume)
                last_prices[symbol] = current_price
                
            logging.info("💤 Очікування 60 секунд до наступного оновлення...")
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("\n🛑 Стрімінг зупинено користувачем.")
            break
        except Exception as e:
            logging.info(f"⚠️ Непередбачена помилка в циклі: {e}")
            time.sleep(10)