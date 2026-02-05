CREATE TABLE IF NOT EXISTS bitcoin_prices (
    id SERIAL PRIMARY KEY,            
    symbol VARCHAR(10),               
    price DECIMAL(18, 8),             
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);