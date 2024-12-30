DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("FromDate"::TEXT, 'YYYYMMDD')       AS CALDT,
    '{{fund}}'                                  AS FUND,
    "StartingValue"::NUMERIC(18,6)              AS STARTING_VALUE,
    "EndingValue"::NUMERIC(18,6)                AS ENDING_VALUE,
    "DepositsWithdrawals"::NUMERIC(18,6)        AS DEPOSITS_WITHDRAWALS
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
