-- one output table per file: this is silver.customer_enriched
SELECT accountid, name
FROM bronze.customer
WHERE _is_current = true
