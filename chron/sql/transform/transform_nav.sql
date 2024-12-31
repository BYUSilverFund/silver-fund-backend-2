DROP TABLE IF EXISTS "XF_{{stage_table}}";
CREATE TABLE "{{xf_table}}" AS 
SELECT DISTINCT
<<<<<<< HEAD
<<<<<<< HEAD
    TO_DATE("ReportDate"::TEXT, 'YYYYMMDD')   AS CALDT,
    '{{fund}}'                                AS FUND,
    "Stock"::NUMERIC(18,6)                    AS STOCK
=======
=======
>>>>>>> 4451abc (added carchar to other report date queries)
    TO_DATE("ReportDate"::VARCHAR(255), 'YYYYMMDD')   AS CALDT,
    '{{fund}}'                          AS FUND,
    "Stock"::NUMERIC(18,6)              AS STOCK
>>>>>>> 4451abc (added carchar to other report date queries)
FROM "{{stage_table}}"
WHERE "ClientAccountID" != 'ClientAccountID';
