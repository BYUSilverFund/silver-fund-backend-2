DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("ReportDate"::VARCHAR(255), 'YYYYMMDD')       AS CALDT,
    '{{fund}}'                              AS FUND,
    "Symbol"                                AS TICKER,
    "ActionID"                              AS ACTION_ID,
    "GrossRate"::NUMERIC(18,6)              AS GROSS_RATE,
    "GrossAmount"::NUMERIC(18,6)            AS GROSS_AMOUNT
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
