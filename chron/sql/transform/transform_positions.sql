DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
<<<<<<< HEAD
<<<<<<< HEAD
    TO_DATE("ReportDate"::TEXT, 'YYYYMMDD') AS CALDT,
=======
    TO_DATE("ReportDate"::VARCHAR(255), 'YYYYMMDD')       AS CALDT,
>>>>>>> 4451abc (added carchar to other report date queries)
=======
    TO_DATE("ReportDate"::VARCHAR(255), 'YYYYMMDD')       AS CALDT,
>>>>>>> 4451abc (added carchar to other report date queries)
    '{{fund}}'                              AS FUND,
    "Symbol"                                AS TICKER,
    "AssetClass"                            AS ASSET_CLASS,
    "Quantity"::NUMERIC(18,6)               AS SHARES,
    "MarkPrice"::NUMERIC(18,6)              AS PRICE,
    "FXRateToBase"::NUMERIC(18,6)           AS FX_RATE
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
