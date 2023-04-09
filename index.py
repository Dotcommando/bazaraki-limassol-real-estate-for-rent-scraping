import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import re
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
LIMASSOL_URL = os.getenv("LIMASSOL")
PAPHOS_URL = os.getenv("PAPHOS")
LARNACA_URL = os.getenv("LARNACA")

def parse_date(date_str):
    if "Yesterday" in date_str:
        date_str = date_str.replace("Yesterday", (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y"))
    elif "Today" in date_str:
        date_str = date_str.replace("Today", datetime.now().strftime("%d.%m.%Y"))
    return int(time.mktime(datetime.strptime(date_str.strip(), "%d.%m.%Y %H:%M").timetuple()))

def get_page_links(soup):
    max_page_number = 1
    pagination = soup.find("ul", class_="number-list")
    if pagination:
        for li in pagination.find_all("li"):
            link = li.find("a", class_="page-number")
            if link:
                page_number = int(link["data-page"])
                max_page_number = max(max_page_number, page_number)
    return [f"/real-estate-to-rent/apartments-flats/?lat=34.69708393413313&lng=33.01509693679961&radius=15000&page={i}" for i in range(1, max_page_number + 1)]

def parse_announcement(soup):
    id_tag = soup.find("a", class_="announcement-block__title")
    description_tag = soup.find("div", class_="announcement-block__description")
    date_tag = soup.find("div", class_="announcement-block__date")
    price_tag = soup.find("meta", itemprop="price")
    currency_tag = soup.find("div", class_="announcement-block__price")

    if not (id_tag and description_tag and date_tag and price_tag and currency_tag):
        return None

    id = id_tag["href"].split("/")[-1]
    url = "https://www.bazaraki.com" + id_tag["href"]
    description = description_tag.text.strip()
    date_str, full_address = date_tag.text.strip().rsplit(",", 1)

    if "," in date_str:
        date_str, city = date_str.rsplit(",", 1)
    else:
        city = ""

    city = city.strip()
    district = full_address.strip()

    if city and district.startswith(city):
        pattern = re.compile(f"^{city}\s*-\s*")
        district = pattern.sub("", district)

    publish_date = parse_date(date_str)
    price = float(price_tag["content"])
    currency = currency_tag.find("b")
    currency = currency.text.strip() if currency else ""

    return {
        "id": id,
        "url": url,
        "description": description,
        "publish_date": publish_date,
        "city": city,
        "district": district,
        "price": price,
        "currency": currency
    }

def scrape_announcements(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    announcements = []
    for announcement_soup in soup.find_all("li", class_="announcement-container"):
        announcement = parse_announcement(announcement_soup)
        if announcement:
            announcements.append(announcement)
    return announcements

def main(city, city_url):
    all_announcements = []

    start_url = BASE_URL + city_url

    response = requests.get(start_url)
    soup = BeautifulSoup(response.text, "html.parser")

    page_links = get_page_links(soup)
    all_announcements += scrape_announcements(start_url)

    for link in page_links:
        url = BASE_URL + link
        all_announcements += scrape_announcements(url)

    announcements_df = pd.DataFrame(all_announcements)
    filename = f"parsed_data/{city.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    announcements_df.to_csv(filename, index=True)

if __name__ == "__main__":
    main("Limassol", LIMASSOL_URL)
    main("Paphos", PAPHOS_URL)
    main("Larnaca", LARNACA_URL)
