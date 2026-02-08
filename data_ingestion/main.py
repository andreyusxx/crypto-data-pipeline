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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ],
    force=True
)

def fetch_btc_price():
    """Отримує актуальну ціну BTC з Binance API"""
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        data = response.json()
        return {'symbol': data['symbol'], 'price': float(data['price'])}
    except Exception as e:
        logging.info(f"❌ Помилка API: {e}")
        return None

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
        logging.info(f"✅ Збережено в БД: {symbol} -> {price}")
    except Exception as e:
        logging.info(f"❌ Помилка БД: {e}")

if __name__ == "__main__":
    logging.info("🚀 Запуск стримінгу даних...")
    last_price = None
    while True:
        try:
            current_data = fetch_btc_price()
            if current_data:
                symbol = current_data['symbol']
                current_price = current_data['price']
                if last_price is not None:
                    diff = current_price - last_price
                    change_percent = abs(current_price - last_price) / last_price * 100
                    trend = "📈" if diff > 0 else "📉" if diff < 0 else "↔️"
                    if change_percent > 50:
                        logging.warning(f"⚠️ АНОМАЛІЯ: Ціна змінилася на {change_percent:.2f}%. Запис ігноровано. Поточна: {current_price}, Попередня: {last_price}")
                        time.sleep(60)
                        continue
                    logging.info(f"Аналіз: {trend} Зміна: {change_percent:.4f}%")

                save_to_db(symbol, current_price)
                last_price = current_price
                
            logging.info("💤 Очікування 60 секунд до наступного оновлення...")
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("\n🛑 Стрімінг зупинено користувачем.")
            break
        except Exception as e:
            logging.info(f"⚠️ Непередбачена помилка в циклі: {e}")
            time.sleep(10)