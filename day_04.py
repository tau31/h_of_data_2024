import polars as pl

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")

complete_orders = orders.join(order_items, on="orderid",  how="inner").join(
    products, on="sku", how="inner")

dawn_orders = complete_orders.filter(
    (pl.col("shipped").str.to_datetime().dt.hour().eq(4)) &
    (pl.col("ordered").eq(pl.col("shipped")))
)

bike_fixer = dawn_orders.filter(
    pl.col("desc").str.contains("Bagel")).join(
    customers.select("customerid", "name", "phone"), on="customerid", how="inner").select("name", "phone")

print(f"The bike fixers phone number is {bike_fixer['phone'].item()}")
