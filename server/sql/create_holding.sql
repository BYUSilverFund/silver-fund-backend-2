CREATE TABLE IF NOT EXISTS holding (
    FUND VARCHAR(255),
    TICKER VARCHAR(255),
    HORIZON_DATE VARCHAR(255),
    TARGET_PRICE NUMERIC(18,4),
    UNIQUE(FUND, TICKER)
);