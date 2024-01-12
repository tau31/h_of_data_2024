# find collector
# hint:
# he owns an entire set of Noah’s collectibles!
# -> define Noah's collectables from products (consider color and other attributes?)
import polars as pl

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")

# initially I thought all products from "Noah's" (e.g. Noah's lunchbox, Noah's jewelry)
# were the collectable items, but this lead me nowhere, since there is no single user
# who has purchased all items

# Solution: a collectable item is normally an expensive item. If we take the the most
# expensive item (Noah's Ark Model (HO Scale), which is a set!) and check who purchased it, we
# get our collector, since only one user has purchased the item.

collectable_set = products.filter(
    pl.col("desc").str.contains("Noah")).sort("wholesale_cost", descending=True).head(1)

collector = order_items.filter(
    pl.col("sku").eq(collectable_set["sku"])
).join(
    orders, on="orderid", how="inner"
).join(
    customers.select("customerid", "name", "phone"), on="customerid", how="inner"
)

print(f"the collector's phone number is {collector['phone'].item()}")
