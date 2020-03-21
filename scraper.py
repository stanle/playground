from bs4 import BeautifulSoup
import requests
import dataset
import pandas

URL = "https://ww2.health.wa.gov.au/reports-and-publications/emergency-department-activity/data?report=ed_activity_now"

response = requests.get(URL)

soup = BeautifulSoup(response.text, "html.parser")

title = soup.find("div", class_ = "ph-head3").text
date = title.split(",")[1]
date = pandas.to_datetime(date)
date = str(date)

tables = soup.find_all("table")

assert len(tables) == 1

table = tables[0]

body = table.find("tbody")

result = []
for row in body.find_all("tr"):
    columns = row.find_all("td")
    r = {c["data-title"]: str(c.text) for c in columns}
    r["hospital"] = r[""]
    del r[""]
    r["date"] = date
    result.append(r)

db = dataset.connect("sqlite:///data.sqlite")
db_table = db["data"]
db_table.insert_many(result)
