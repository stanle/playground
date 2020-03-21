from bs4 import BeautifulSoup
import requests
import dataset
import pandas
import schedule
import time

URL = "https://ww2.health.wa.gov.au/reports-and-publications/emergency-department-activity/data?report=ed_activity_now"

db = dataset.connect("sqlite:///data.sqlite")
db_table = db["data"]


def scrape():
    response = requests.get(URL)

    soup = BeautifulSoup(response.text, "html.parser")

    title = soup.find("div", class_="ph-head3").text
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

    print(10 * "#")
    print(result)
    db_table.insert_many(result)


def run_scheduled():
    schedule.every().hour.do(scrape)

    while True:
        schedule.run_pending()
        time.sleep(1)


run_scheduled()
