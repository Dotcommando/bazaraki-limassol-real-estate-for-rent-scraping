import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

def parse_date(date_str):
    today = datetime.now()
    if "Yesterday" in date_str:
        date_str = date_str.replace("Yesterday", (today - timedelta(days=1)).strftime("%d.%m.%Y"))
    elif "Today" in date_str:
        date_str = date_str.replace("Today", today.strftime("%d.%m.%Y"))
    return int(time.mktime(datetime.strptime(date_str.strip(), "%d.%m.%Y %H:%M").timetuple()))

# Запрос к веб-странице с объявлениями
url = "https://www.bazaraki.com/real-estate-to-rent/apartments-flats/?lat=34.69708393413313&lng=33.01509693679961&radius=15000"
response = requests.get(url)
html = response.text

# Парсинг HTML
soup = BeautifulSoup(html, "html.parser")
announcement_containers = soup.find_all("li", class_="announcement-container")

# Извлечение данных
announcements = []

for container in announcement_containers:
    title_tag = container.find("a", class_="announcement-block__title")
    description_tag = container.find("div", class_="announcement-block__description")
    date_tag = container.find("div", class_="announcement-block__date")
    price_tag = container.find("meta", itemprop="price")
    currency_tag = container.find("div", class_="announcement-block__price").find("b")

    title = title_tag.text.strip()
    url = title_tag["href"]
    description = description_tag.text.strip()
    date_str, address = date_tag.text.strip().split(",", 1)
    publish_date = parse_date(date_str)
    address = address.strip()
    price = float(price_tag["content"])
    currency = currency_tag.text.strip()

    announcements.append({"title": title, "description": description, "url": url, "price": price, "currency": currency, "publish_date": publish_date, "address": address})

# Преобразование данных в формат, удобный для использования с NumPy и Pandas
announcements_np = np.array(announcements, dtype=object)
announcements_df = pd.DataFrame(announcements, columns=["title", "description", "url", "price", "currency", "publish_date", "address"])

# Вывод DataFrame в виде таблицы
print(announcements_df.to_string())
