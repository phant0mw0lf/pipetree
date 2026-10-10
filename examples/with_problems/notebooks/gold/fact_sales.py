# notebooks/gold/fact_sales.py
# one output table per file: this is gold.fact_sales
# reads at the top, per convention, so depends_on: auto can find them
from pyspark.sql import functions as F

orders = spark.table("silver.orders")  # noqa: F821 - `spark` is injected by the adapter
customer = spark.table("gold.dim_customer")  # noqa: F821

# a missing dim_customer match (order_id 104's customer doesn't exist)
# resolves to the -1 unknown member instead of leaving a NULL foreign key
result = orders.join(customer, orders.customer_accountid == customer.accountid, "left").select(
    orders.order_id,
    orders.amount,
    orders.order_date,
    F.coalesce(customer.accountid, F.lit(-1)).alias("customer_accountid"),
)
