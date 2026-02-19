# 🪙 Crypto Data Warehouse & Real-time Analytics

Це комплексна **Data Engineering** платформа для автоматизованого збору, обробки та візуалізації крипто-аналітики. 

---

## 🏗 **Архітектура системи**
Проект реалізує повний життєвий цикл даних за принципом **Medallion Architecture**:

* **Extract**: **Apache Airflow** збирає сирі дані з **Binance API**.
* **Load**: Дані зберігаються в **MinIO** (S3-like storage) як **Bronze** шар.
* **Transfer**: Дані завантажуються в **PostgreSQL** (**Silver** шар).
* **Transform**: **dbt** перетворює сирі дані на аналітичні вітрини (**Gold** шар).
* **Analyze & Visualize**: 
    * **Streamlit**: Кастомний дашборд на Python.
    * **Metabase**: Професійна BI-платформа.
* **Alerting**: **Telegram-бот** сповіщає про зміни цін (>5%) та помилки.

---

## 🚦 **Порти сервісів**
| Сервіс | Адреса | Призначення |
| :--- | :--- | :--- |
| **Airflow** | `http://localhost:8080` | Оркестрація пайплайнів |
| **Streamlit** | `http://localhost:8501` | Крипто-дашборд |
| **Metabase** | `http://localhost:3000` | BI-аналітика |
| **MinIO** | `http://localhost:9001` | Об'єктне сховище |

---

## 💻 **Як запустити проект**

### **1. Клонування проекту**
```bash
git clone <url_твого_репозиторію>
cd <назва_папки_проекту>
```
### **2. Налаштування змінних оточення**
Telegram
TELEGRAM_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_id_here

Postgres
DB_USER=user
DB_PASSWORD=password
DB_NAME=crypto_db
AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://user:password@db:5432/crypto_db

MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_BUCKET_NAME=crypto-raw-data
MINIO_ENDPOINT=minio:9000
