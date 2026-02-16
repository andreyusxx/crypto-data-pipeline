import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "database": os.getenv("DB_NAME", "crypto_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT",'BNBUSDT','ADAUSDT', 'DOGEUSDT', 'XRPUSDT', 'DOTUSDT']
UPDATE_INTERVAL = 60  