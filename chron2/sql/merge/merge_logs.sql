INSERT INTO logs
(
    date,
    fund,
    logs
)
VALUES
(
    '{{date}}',
    '{{fund}}',
    '{{logs}}'
)
ON CONFLICT (date, fund)
DO UPDATE SET
    logs = EXCLUDED.logs;
