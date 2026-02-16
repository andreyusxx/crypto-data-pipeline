CREATE TABLE IF NOT EXISTS prices_btc (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS prices_eth (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prices_sol (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS prices_bnb (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS prices_ada (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS prices_doge (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS prices_xrp (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS prices_dot (
    id SERIAL PRIMARY KEY,
    price DECIMAL(18, 8),
    volume DECIMAL(18, 8),
    event_time BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
