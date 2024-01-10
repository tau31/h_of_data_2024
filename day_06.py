import polars as pl

# hints:
# - Staten Island
# - I only have ten or eleven cats, and anyway
# they are getting quite old now, so I doubt they’d care about some old rug.”
# -> buys a lot of cat products

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")
staten_islanders = customers.filter(
    pl.col("citystatezip").str.contains("Staten Island"))

orders_with_info = orders.join(
    order_items.select("orderid", "sku", "qty"), on="orderid", how="inner").join(
        products.select("sku", "desc", "wholesale_cost"), on="sku", how="inner")

staten_cat_orders = orders_with_info.filter(pl.col("customerid").is_in(staten_islanders.select("customerid"))).select(
    pl.exclude("items", "sku")
).filter(pl.col("desc").str.to_lowercase().str.contains("cat"))

cat_orders_agg = staten_cat_orders.with_columns(
    (pl.col("wholesale_cost") * pl.col("qty")).alias("total_cat_prods")
).group_by("customerid").agg(pl.sum("total_cat_prods").alias("total_spent")).sort("total_spent", descending=True)

cat_lady = cat_orders_agg.filter(pl.col("total_spent").eq(
    pl.max("total_spent"))).join(staten_islanders, on="customerid", how="inner")

print(f"the cat lady's phone number is {cat_lady['phone'].item()}")
