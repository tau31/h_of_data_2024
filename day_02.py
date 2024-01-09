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

f"the detective's phone number is {detective_id['phone'].item()}"
