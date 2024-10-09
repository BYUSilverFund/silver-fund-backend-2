CREATE TABLE IF NOT EXISTS TRADES (
    CALDT DATE NOT NULL,
    FUND VARCHAR(255) NOT NULL,
    TICKER VARCHAR(255) NOT NULL,
    TRANSACTION_ID VARCHAR(255) NOT NULL,
    ASSET_CLASS VARCHAR(255) NOT NULL,
    SHARES NUMERIC(18,6) NOT NULL,
    PRICE NUMERIC(18,6) NOT NULL,
    BUY_SELL VARCHAR(255) NOT NULL,
    CONSTRAINT UNIQUE_TRADE UNIQUE (CALDT, FUND, TICKER, TRANSACTION_ID)
);
