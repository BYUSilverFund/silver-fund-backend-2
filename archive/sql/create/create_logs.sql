CREATE TABLE IF NOT EXISTS logs (
    date DATE,
    fund VARCHAR(255),
    logs TEXT,
    CONSTRAINT logs_constraint UNIQUE (date, fund)
);