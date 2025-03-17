DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("ReportDate"::VARCHAR(255), 'YYYYMMDD')       AS CALDT,
    '{{fund}}'                              AS FUND,
    "Symbol"                                AS TICKER,
    "AssetClass"                            AS ASSET_CLASS,
    "Quantity"::NUMERIC(18,6)               AS SHARES,
    "MarkPrice"::NUMERIC(18,6)              AS PRICE,
    "FXRateToBase"::NUMERIC(18,6)           AS FX_RATE
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';