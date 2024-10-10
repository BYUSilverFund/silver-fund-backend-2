DROP TABLE IF EXISTS "{{xf_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("ReportDate", 'YYYYMMDD')       AS CALDT,
    "Symbol"                                AS TICKER,
    "MarkPrice"::NUMERIC(18,6)              AS ENDING_VALUE
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID'
AND "Symbol" = 'IWV';
