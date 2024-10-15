INSERT INTO logs
(
    CALDT,
    FUND,
    LOGS
)
VALUES
(
    '{{date}}',
    '{{fund}}',
    '{{logs}}'
)
ON CONFLICT (CALDT, FUND)
DO UPDATE SET
    CALDT = EXCLUDED.CALDT,
    FUND = EXCLUDED.FUND,
    LOGS = EXCLUDED.LOGS;
