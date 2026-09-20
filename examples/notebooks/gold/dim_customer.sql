-- one output table per file: this is gold.dim_customer
SELECT accountid, name
FROM silver.customer_enriched
