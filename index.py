import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import Levenshtein
import re
import random
import sys

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
MIN_DELAY = float(os.getenv("MIN_DELAY", 100)) / 1000
MAX_DELAY = float(os.getenv("MAX_DELAY", 5000)) / 1000

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}

def random_delay(min_delay, max_delay):
    time.sleep(random.uniform(min_delay, max_delay))

def get_response(url):
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error: HTTP status code {response.status_code} for URL {url}")
    return response

def print_progress(current, total, city):
    sys.stdout.write("\r\nScraped {} of {} ads in {}".format(current, total, city))
    sys.stdout.flush()

def parse_date(date_str):
    if "Yesterday" in date_str:
        date_str = date_str.replace("Yesterday", (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y"))
    elif "Today" in date_str:
        date_str = date_str.replace("Today", datetime.now().strftime("%d.%m.%Y"))
    return int(time.mktime(datetime.strptime(date_str.strip(), "%d.%m.%Y %H:%M").timetuple()))

def get_page_links(soup, lat=None, lng=None, radius=None):
    page_links = []
    pagination = soup.find("div", class_="pagination")

    if pagination:
        last_page = int(pagination.find_all("a")[-2].text)

        for i in range(2, last_page + 1):
            if lat and lng and radius:
                link = f"/real-estate-to-rent/apartments-flats/?page={i}&lat={lat}&lng={lng}&radius={radius}"
            else:
                url_with_page_param = soup.find("a", href=True, text=str(i))
                if url_with_page_param:
                    link = url_with_page_param["href"]
                else:
                    break
            page_links.append(link)

    return page_links

def parse_announcement(soup):
    id_tag = soup.find("a", class_="announcement-block__title")
    description_tag = soup.find("div", class_="announcement-block__description")
    date_tag = soup.find("div", class_="announcement-block__date")
    price_tag = soup.find("meta", itemprop="price")
    currency_tag = soup.find("div", class_="announcement-block__price")
    ad_id_tag = soup.find("div", class_="announcement-block__favorites")

    if not (id_tag and description_tag and date_tag and price_tag and currency_tag and ad_id_tag):
        return None

    id = id_tag["href"].split("/")[-1]
    url = BASE_URL + id_tag["href"]
    description = description_tag.text.strip()
    date_str, full_address = date_tag.text.strip().rsplit(",", 1)
    ad_id = ad_id_tag["data-id"]

    if "," in date_str:
        date_str, city = date_str.rsplit(",", 1)
    else:
        city = ""

    city = city.strip()
    district = full_address.strip()

    if city:
        parts = [part.strip() for part in district.split('-')]

        if len(parts) == 2 and Levenshtein.distance(parts[0], city) <= 1:
            district = parts[1]

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
        "currency": currency,
        "ad_id": ad_id
    }

def scrape_announcements(url, parsed_urls):
    response = get_response(url)
    soup = BeautifulSoup(response.text, "html.parser")
    announcements = []
    for announcement_soup in soup.find_all("li", class_="announcement-container"):
        announcement = parse_announcement(announcement_soup)
        if announcement and announcement['url'] not in parsed_urls:
            announcements.append(announcement)
            parsed_urls.add(announcement['url'])
    return announcements

def get_city_regions():
    city_regions = {}
    for key, value in os.environ.items():
        if key.endswith("_URL") and key != "BASE_URL":
            city = key[:-4]
            if "_" in city:
                city, region = city.rsplit("_", 1)
            else:
                region = None

            if city not in city_regions:
                city_regions[city] = {}

            if region:
                city_regions[city][region] = value
            else:
                city_regions[city]["main"] = value

    return city_regions

def extract_additional_data(url):
    response = get_response(url)
    soup = BeautifulSoup(response.text, "html.parser")

    data = {}
    ul = soup.find("ul", class_="chars-column")

    if ul:
        for li in ul.find_all("li"):
            key = li.find("span", class_="key-chars")
            value = li.find("a", class_="value-chars") or li.find("span", class_="value-chars")
            if key and value:
                column_name = re.sub(r'\s+', ' ', key.text.strip()).lower().replace(" ", "-").strip(":")
                data[column_name] = value.text.strip()

    details_div = soup.find("div", class_="announcement__details")

    if details_div:
        date_meta = details_div.find("span", class_="date-meta")

        if date_meta:
            date_str = date_meta.text.strip().replace("Posted: ", "")
            data["publish_date"] = parse_date(date_str)

    random_delay(MIN_DELAY, MAX_DELAY)
    return data

def main(city, regions, filename, parsed_urls):
    all_announcements = []

    for region, city_url in regions.items():
        print(f"Scraping {city} {region}")

        start_url = BASE_URL + city_url

        response = get_response(start_url)
        soup = BeautifulSoup(response.text, "html.parser")

        lat_tag = soup.find("input", id="lat")
        lng_tag = soup.find("input", id="lng")
        radius_tag = soup.find("input", id="radius")

        page_links = get_page_links(soup, lat_tag["value"] if lat_tag else None, lng_tag["value"] if lng_tag else None, radius_tag["value"] if radius_tag else None)

        all_announcements += scrape_announcements(start_url, parsed_urls)

        for link in page_links:
            url = BASE_URL + link
            all_announcements += scrape_announcements(url, parsed_urls)

    announcements_df = pd.DataFrame(all_announcements)

    if os.path.exists(filename):
        existing_df = pd.read_csv(filename)
        combined_df = pd.concat([existing_df, announcements_df], ignore_index=True)
    else:
        combined_df = announcements_df

    combined_df.to_csv(filename, index=False)

    visited_ads = 0
    ads_to_save = 5
    total_ads = len(combined_df)

    for index, announcement in combined_df.iterrows():
        if "area" not in announcement or pd.isnull(announcement["area"]):
            url = announcement["url"]
            additional_data = extract_additional_data(url)
            for key, value in additional_data.items():
                combined_df.at[index, key] = value

            visited_ads += 1
            if visited_ads % ads_to_save == 0:
                combined_df.to_csv(filename, index=False)
                print_progress(visited_ads, total_ads, city)

    combined_df.to_csv(filename, index=False)

if __name__ == "__main__":
    parsed_urls = set()
    city_regions = get_city_regions()
    for city, regions in city_regions.items():
        filename = f"parsed_data/{city.lower()}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        main(city, regions, filename, parsed_urls)
