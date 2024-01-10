# ###
# The Investigator

# Sarah brought a cashier over. She said, “Joe here says that one of our customers is a skilled private investigator.”

# Joe nodded, “They showed me their business card, and that’s what it said. Skilled Private Investigator. And their phone number was their last name spelled out. I didn’t know what that meant, but apparently before there were smartphones, people had to remember phone numbers or write them down. If you wanted a phone number that was easy-to-remember, you could get a number that spelled something using the letters printed on the phone buttons: like 2 has “ABC”, and 3 “DEF”, etc. And I guess this person had done that, so if you dialed the numbers corresponding to the letters in their name, it would call their phone number!

# “I thought that was pretty cool. But I don’t remember their name, or anything else about them for that matter. I couldn’t even tell you if they were male or female.”

# Sarah said, “This person seems like they are skilled at investigation. I need them to find Noah’s rug before the Hanukkah dinner. I don’t know how to contact them, but apparently they shop here at Noah’s Market.”

# She nodded at the USB drive in your hand.

# “Can you find this investigator’s phone number?”
###

import polars as pl

customers = pl.read_csv("./usb/noahs-customers.csv", try_parse_dates=True)

dt = customers.select(pl.col("name", "phone")).with_columns(
    pl.col("name").str.split(by=" ").list.last(
    ).str.to_uppercase().str.extract_all("\w+").list.explode().alias("last_name"),
    pl.col("phone").str.replace_all("-", "").alias("phone_string")
)


def get_mapping():
    mapping = pl.DataFrame({
        "number": range(2, 10),
        "letters":  [
            ["A", "B", "C"],
            ["D", "E", "F"],
            ["G", "H", "I"],
            ["J", "K", "L"],
            ["M", "N", "O"],
            ["P", "Q", "R", "S"],
            ["T", "U", "V"],
            ["W", "X", "Y", "Z"]
        ]
    }).explode("letters").select(pl.col("letters"), pl.col("number").cast(pl.String))
    return mapping


def convert_name_to_number(names):
    mapping = get_mapping()
    number_l = []
    for i_name in names:
        number = []
        for char in i_name:
            res = mapping.filter(pl.col("letters").str.contains(
                char)).select(pl.col("number")).item()
            number.append(res)
        number = ''.join(number)
        number_l.append(number)
    return number_l


dt = dt.with_columns([
    pl.col("last_name").map_batches(
        lambda names: convert_name_to_number(names)).explode().alias("phone_from_name")
])

detective_id = dt.filter(pl.col("phone_string") == pl.col(
    "phone_from_name"))

print(f"the detective's phone number is {detective_id['phone'].item()}")
