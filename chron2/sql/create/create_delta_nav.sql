CREATE TABLE IF NOT EXISTS DELTA_NAV (
    CALDT DATE NOT NULL,
    FUND VARCHAR(255) NOT NULL,
    STARTING_VALUE NUMERIC(18,6) NOT NULL,
    ENDING_VALUE NUMERIC(18,6) NOT NULL,
    DEPOSITS_WITHDRAWALS NUMERIC(18,6) NOT NULL,
    CONSTRAINT UNIQUE_DELTA_NAV UNIQUE (CALDT, FUND)
);
