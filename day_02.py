# The Contractor

# Thanks to your help, Sarah called the investigator that afternoon. The investigator went directly to the cleaners to see if they could get any more information about the unclaimed rug.

# While they were out, Sarah said, “I tried cleaning the rug myself, but there was this snail on it that always seemed to leave a trail of slime behind it. I spent a few hours cleaning it, and the next day the slime trail was back.”

# When the investigator returned, they said, “Apparently, this cleaner had a special projects program, where they outsourced challenging cleaning projects to industrious contractors. As they’re right across the street from Noah’s, they usually talked about the project over coffee and bagels at Noah’s before handing off the item to be cleaned. The contractors would pick up the tab and expense it, along with their cleaning supplies.

# “So this rug was apparently one of those special projects. The claim ticket said ‘2017 JP’. ‘2017’ is the year the item was brought in, and ‘JP’ is the initials of the contractor.

# “But they stopped outsourcing a few years ago, and don’t have contact information for any of these workers anymore.”

# Sarah first seemed hopeless, and then glanced at the USB drive you had just put back in her hand. She said, “I know it’s a long shot, but is there any chance you could find their phone number?”

import polars as pl

customers = pl.read_csv("usb/noahs-customers.csv")
orders = pl.read_csv("usb/noahs-orders.csv")
order_items = pl.read_csv("usb/noahs-orders_items.csv")
products = pl.read_csv("usb/noahs-products.csv")


jp = customers.with_columns(
    pl.col("name").str.split(" ").map_elements(
        lambda x: x[0][0] + x[-1][0]).alias("initials")
).filter(pl.col("initials") == "JP").select(pl.col("customerid"), pl.col("initials"), pl.col("phone"))


jp_orders = orders.join(jp.select(pl.col("customerid")), on="customerid", how="inner").filter(
    pl.col("ordered").str.to_date("%Y-%m-%d %H:%M:%S").dt.year() == 2017)

rug_cleaner = products.filter(
    pl.col("desc").str.to_lowercase().str.contains("^rug"))

rug_cleaner_orders_jp = jp_orders.join(order_items, on="orderid", how="inner").join(
    rug_cleaner, on="sku", how="inner")

contractor = customers.filter(pl.col("customerid") == rug_cleaner_orders_jp.select(
    pl.col("customerid")).unique())

contractor.write_csv("usb/contractor.csv")

print(f"the contractor phone number is {contractor['phone'][0]}")
