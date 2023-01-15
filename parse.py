import asyncio
import copy
import csv
import os
import time
from datetime import datetime
from urllib.parse import urljoin

import aiohttp as aiohttp
import httpx
from httpx import AsyncClient
import requests
from bs4 import BeautifulSoup

DOMAIN_URL = "https://djinni.co"
PYTHON_POSITIONS_URL = DOMAIN_URL + "/jobs/?primary_keyword=Python"

technologies = {
    "Python": 0,
    "SQL": 0,
    "REST": 0,
    "API": 0,
    "GIT": 0,
    "Django": 0,
    "docker": 0,
    "Postgresql": 0,
    "JS": 0,
    "JavaScript": 0,
    "AWS": 0,
    "Flask": 0,
    "HTML": 0,
    "redis": 0,
    "linux": 0,
    "fullstack": 0,
    "Artificial intelligence": 0,
    " AI ": 0,
    "MySQL": 0,
    "OOP": 0,
    "react": 0,
    "CSS": 0,
    "MongoDB": 0,
    "FastAPI": 0,
    "DRF": 0,
    "Machine Learning": 0,
    "angular": 0,
    "NoSQL": 0,
    "pytest": 0,
    "networking": 0,
    "SQLAlchemy": 0,
    "microservice": 0,
    "pandas": 0,
    "algorithms": 0,
    "aiohttp": 0,
    "Azure": 0,
    "Graphql": 0,
    "asyncio": 0,
    "unittest": 0,
    "beautifulsoup": 0,
    "unix": 0,
    "Tornado": 0,
    "opencv": 0,
    "numpy": 0,
    "multiprocessing": 0,
    "SQLite": 0,
    "requests": 0,
    "scraping": 0,
    "regex": 0,
    "decorators": 0,
    "generators": 0,
    "test driven development": 0,
    "twisted": 0,
    "iterators": 0
}

junior_technologies = copy.deepcopy(technologies)
middle_technologies = copy.deepcopy(technologies)
senior_technologies = copy.deepcopy(technologies)


def write_result(info_technologies: dict) -> None:
    folder_path = os.path.abspath("data_storage")
    if "years_of_experience" in info_technologies:
        filename = os.path.join(
            folder_path,
            f"{info_technologies['years_of_experience']}_{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}.csv"
        )
    else:
        filename = os.path.join(
            folder_path,
            f"all_levels_{datetime.now().strftime('%Y-%m-%d %H_%M_%S')}.csv"
        )
    with open(filename, "w") as f:
        writer = csv.writer(f)
        for key, value in info_technologies.items():
            if key != "years_of_experience" and value != 0:
                writer.writerow([key, value])


def scrape_technologies(soup, some_technologies):
    if soup.select_one(".profile-page-section") is not None:
        info = soup.select_one(".profile-page-section").text
        for item in technologies:
            if item.lower() in info.lower():
                some_technologies[item] += 1
                technologies[item] += 1


def scrape_for_experience_group(index, soup):
    if index.isdigit():
        if int(index) <= 2:
            junior_technologies["years_of_experience"] = "junior"
            scrape_technologies(soup, junior_technologies)
        elif int(index) >= 5:
            senior_technologies["years_of_experience"] = "senior"
            scrape_technologies(soup, senior_technologies)
        else:
            middle_technologies["years_of_experience"] = "middle"
            scrape_technologies(soup, middle_technologies)
    else:
        junior_technologies["years_of_experience"] = "junior"
        scrape_technologies(soup, junior_technologies)


#API
def define_experience_group(position_link: str) -> None:
    link = urljoin(DOMAIN_URL, position_link)
    response = httpx.get(link)
    soup = BeautifulSoup(response.content, "html.parser")

    text_with_years = soup.select_one(".job-additional-info").get_text(strip=True, separator=" ")
    if "років досвіду" in text_with_years:
        experience_index = text_with_years[text_with_years.find("років досвіду") - 2]
    elif "роки досвіду" in text_with_years:
        experience_index = text_with_years[text_with_years.find("роки досвіду") - 2]
    else:
        experience_index = text_with_years[
                           (text_with_years.find("досвіду") - 4):(text_with_years.find("досвіду") - 1)
                           ]

    scrape_for_experience_group(experience_index, soup)


def get_num_pages(page_soup: BeautifulSoup) -> int:
    pagination = page_soup.select_one(".pagination")

    if pagination is None:
        return 1

    return int(pagination.select("a.page-link")[-2].text)


def get_single_page_position(page_soup: BeautifulSoup):
    positions = page_soup.select(".profile")
    return [define_experience_group(position_link.get("href")) for position_link in positions]


#API
async def scrape_links_of_positions(page, client: AsyncClient):
    page = await client.get(PYTHON_POSITIONS_URL, params={"page": page})
    soup = BeautifulSoup(page.content, "html.parser")
    return soup


#API
async def get_first_page():
    response = httpx.get(PYTHON_POSITIONS_URL)
    first_page_soup = BeautifulSoup(response.content, "html.parser")

    num_pages = get_num_pages(first_page_soup)

    get_single_page_position(first_page_soup)

    async with AsyncClient() as client:
        all_positions = await asyncio.gather(
            *[scrape_links_of_positions(page, client) for page in range(2, num_pages + 1)]
        )
        for position in all_positions:
            get_single_page_position(position)

    write_result(junior_technologies)
    write_result(middle_technologies)
    write_result(senior_technologies)
    write_result(technologies)


if __name__ == '__main__':
    start_time = time.perf_counter()
    asyncio.run(get_first_page())
    end_time = time.perf_counter()
    print("Elapsed:", end_time - start_time)
