DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
    TO_DATE("ReportDate", 'YYYYMMDD')       AS CALDT,
    '{{fund}}'                              AS FUND,
    "Symbol"                                AS TICKER,
    "TransactionID"                         AS TRANSACTION_ID,
    "AssetClass"                            AS ASSET_CLASS,
    "Quantity"::NUMERIC(18,6)               AS SHARES_TRADED,
    "TradePrice"::NUMERIC(18,6)             AS PRICE_TRADED,
    "Buy/Sell"                              AS BUY_SELL
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
