# hint
# He said, ‘I got the same thing!’ We laughed about it and wound up swapping
# items because I wanted the color he got.
# -> both customers purchased the same item at the same time (or aproximately)
# -> item can have different colors

import polars as pl

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")

# get products with colors: pattern is e.g. (gree)
color_prods = products.filter(
    pl.col("desc").str.to_lowercase().str.contains(r"\(\w+\)"))

orders_to_inspect = order_items.join(
    color_prods.select("sku", "desc"), on="sku", how="inner")

bargain_hunter = pl.read_csv("usb/bargain_hunter.csv")

prod_info_expr = (pl.col("desc").str.replace(r" \(\w+\)",                                             "").str.strip_chars().alias("product"),
                  pl.col("desc").str.extract_all(
    r"\(\w+\)").explode().str.extract_all("\w+").explode().alias("color"),
    pl.col("shipped").str.to_datetime(
        "%Y-%m-%d %H:%M:%S").cast(pl.Date).alias("pickup_date"))

possible_orders = orders.select(
    "orderid", "customerid", "ordered", "shipped"
).join(
    orders_to_inspect, on="orderid", how="inner"
).with_columns(prod_info_expr)

bargain_hunter_orders = possible_orders.filter(
    pl.col("customerid").eq(bargain_hunter["customerid"])
).select(
    "orderid", "shipped", "qty", "desc"
).with_columns(prod_info_expr)

cutie_order = possible_orders.join(
    bargain_hunter_orders.select("pickup_date", "product", "color"),
    on=["pickup_date", "product", "color"],
    how="anti"
).join(
    bargain_hunter_orders.select("shipped", "pickup_date", "product"), on=["pickup_date", "product"], how="inner"
).with_columns(
    pl.col("shipped").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    pl.col("shipped_right").str.to_datetime("%Y-%m-%d %H:%M:%S"),
).with_columns(
    (pl.col("shipped") - pl.col("shipped_right")
     ).dt.total_seconds().abs().alias("time_diff")
).sort("time_diff").head(1).join(
    customers.select("customerid", "name", "phone"),
    on="customerid",
    how="inner"
)

print(f"This is the Cutie's phone number {cutie_order['phone'].item()}")
