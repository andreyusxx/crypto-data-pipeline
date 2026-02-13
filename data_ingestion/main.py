import requests
import psycopg2
import time
import sys
import logging
import os
from dotenv import load_dotenv
import psycopg2
from config import DB_CONFIG, SYMBOLS, UPDATE_INTERVAL
from logging.handlers import RotatingFileHandler
from datetime import datetime
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler("pipeline.log",maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'),
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
        logging.error(f"❌ Помилка API: {e}")
        return []

def save_to_db(symbol, price,volume,event_time):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO bitcoin_prices (symbol, price, volume, event_time) VALUES (%s, %s, %s, %s)",
            (symbol, price, volume, event_time)
        )
        
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"✅ Збережено в БД: {symbol} -> {price}, Об'єм: {volume}, (Час події: {event_time})")
    except Exception as e:
        logging.info(f"❌ Помилка БД: {e}")

def run_maintenance():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("CALL clean_old_data();") # Викликаємо твою процедуру
        conn.commit()
        cur.close()
        conn.close()
        logging.info("🧹 Обслуговування бази: старі дані видалено.")
    except Exception as e:
        logging.error(f"❌ Помилка під час очищення даних: {e}")

def check_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        logging.info("🚀 З'єднання з БД успішне!")
        return True
    except Exception as e:
        logging.error(f"⚠️ БД не доступна: {e}")
        return False

if __name__ == "__main__":
    logging.info("🚀 Запуск стримінгу даних...")
    if not check_db_connection():
        exit(1)
    last_prices = {}
    maintenance_done = False

    while True:
        try:
            prices_data = fetch_crypto_prices(SYMBOLS)
            now = datetime.now()
            if now.hour == 3 and now.minute == 0 and not maintenance_done:
                logging.info("⏰ Настав час нічного обслуговування...")
                run_maintenance()
                maintenance_done = True
            if now.hour == 4:
                maintenance_done = False

            if not prices_data:
                logging.warning("⚠️ Дані від API не отримані.")

            for data in prices_data:
                symbol = data['symbol']
                current_price = float(data['lastPrice'])
                current_volume = float(data['volume'])
                event_time = data['closeTime']


                if symbol in last_prices:
                    prew_price = last_prices[symbol]
                    diff = current_price - prew_price
                    change_percent = abs(current_price - prew_price) / prew_price * 100
                    trend = "📈" if diff > 0 else "📉" if diff < 0 else "↔️"

                    
                    if change_percent > 50:
                        logging.warning(f"⚠️ АНОМАЛІЯ: Ціна змінилася на {change_percent:.2f}%. Запис ігноровано. Поточна: {current_price}, Попередня: {last_price}")
                        time.sleep(UPDATE_INTERVAL)
                        continue
                    logging.info(f"Аналіз [{symbol}]: {trend} Зміна: {change_percent:.4f}%")

                save_to_db(symbol, current_price, current_volume,event_time)
                last_prices[symbol] = current_price
                
            logging.info(f"💤 Очікування {UPDATE_INTERVAL} секунд до наступного оновлення...")
            time.sleep(UPDATE_INTERVAL)
        except KeyboardInterrupt:
            logging.info("\n🛑 Стрімінг зупинено користувачем.")
            break
        except Exception as e:
            logging.info(f"⚠️ Непередбачена помилка в циклі: {e}")
            time.sleep(10)