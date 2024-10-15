DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("ReportDate", 'YYYYMMDD')   AS CALDT,
    '{{fund}}'                          AS FUND,
    "Stock"::NUMERIC(18,6)              AS STOCK
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
