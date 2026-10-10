-- one output table per file: this is gold.dim_customer
-- Deliberately broken: `region` is not a column of silver.customer_enriched,
-- so this table fails and gold.fact_sales (which reads it) is upstream_failed.
SELECT accountid, name, region
FROM silver.customer_enriched
