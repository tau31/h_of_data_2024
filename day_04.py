# The Neighbor

# Sarah and the investigator were very impressed with your data skills, as you were able to figure out the phone number of the contractor. They called up the cleaning contractor straight away and asked about the rug.

# “Oh, yeah, I did some special projects for them a few years ago. I remember that rug unfortunately. I managed to clean one section, which revealed a giant spider that startled me whenever I tried to work on it.

# “I already had a fear of spiders before this, but this spider was so realistic that I had a hard time making any more progress. I kept expecting the cleaners would call for the rug, but they never did. I felt so bad about it, I couldn’t face them, and of course they never gave me another project.

# “At last I couldn’t deal with the rug taking up my whole bathtub, so I gave it to this guy who lived in my neighborhood. He said that he was naturally intuitive because he was a Cancer born in the year of the Rabbit, so maybe he was able to clean it.

# “I don’t remember his name. Last time I saw him, he was leaving the subway and carrying a bag from Noah’s. I swore I saw a spider on his hat.”

# Can you find the phone number of the person that the contractor gave the rug to?

import polars as pl
import wikipedia as wiki
import pandas
from bs4 import BeautifulSoup
# hints: Cancer born in the year of the rabbit
# Cancer timeframe:  June 22 to about July 22
# Year of the rabbit:

customers = pl.read_csv("usb/noahs-customers.csv")
customers = customers.with_columns(pl.col("birthdate").str.to_date("%Y-%m-%d"))

# get rabbit year ranges
rabbit_year_html = wiki.page("Rabbit (zodiac)").html().encode("UTF-8")
table_html = BeautifulSoup(rabbit_year_html).find(
    'table', attrs={"class": "wikitable"}).find_all('tr')

# https://scrapfly.io/blog/how-to-scrape-tables-with-beautifulsoup/
header = []
rows = []
for i, row in enumerate(table_html):
    if i == 0:
        header = [x.text.strip() for x in row.find_all('th')]
    else:
        rows.append([x.text.strip() for x in row.find_all('td')])

rabbit_year_ranges = pl.DataFrame(rows, schema=["start_date", "end_date", "not_needed"]).select(
    pl.exclude("not_needed")).with_columns(pl.all().str.to_date("%d %B %Y"))

bd_ranges = customers.select(
    pl.col("birthdate").min().alias("min_db"),
    pl.col("birthdate").max().alias("max_bd")
)


possible_rabbit_range = rabbit_year_ranges.filter(
    pl.col("start_date") >= bd_ranges["min_db"],
    pl.col("end_date") <= bd_ranges["max_bd"])

possible_rabbit_range.select(pl.col("end_date")).to_struct()

customers.filter(pl.col("birthdate").is_between(possible_rabbit_range.select(
    pl.col("start_date")).to_struct(), possible_rabbit_range.select(pl.col("end_date")).to_struct()))


rabbit_year_ranges["start_date"].to_list()
