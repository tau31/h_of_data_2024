import polars as pl

# hint:
# shops every sale at Noah’s Market.
# In fact I like to tease her that Noah actually loses money whenever she comes in the store.
# -> order total is lower then the expected total form wholesale_cost

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")

orders_total = orders.select("customerid", "orderid", "total")
orders_expected_total = order_items.join(products.select("sku", "wholesale_cost"), on="sku", how="inner").groupby(
    "orderid").agg(pl.sum("wholesale_cost").alias("expected_total"))

total_diff = orders_total.join(orders_expected_total, on="orderid", how="inner").with_columns(
    (pl.col("total") - pl.col("expected_total")).alias("diff")
)

bargain_hunter = total_diff.group_by("customerid").agg(pl.sum("diff").alias("net_diff")).sort("net_diff").filter(
    pl.col("net_diff").eq(pl.min("net_diff"))
).join(customers.select("customerid", "name", "phone"), on="customerid", how="inner")

print(f"The bargain hunter's phone number is {bargain_hunter['phone'].item()}")
