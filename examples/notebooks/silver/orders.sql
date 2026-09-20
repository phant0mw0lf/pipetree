-- one output table per file: this is silver.orders
SELECT order_id, customer_accountid, amount, order_date
FROM bronze.orders
