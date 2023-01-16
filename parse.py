import asyncio
import copy
import csv
import os
import time
from datetime import datetime
from urllib.parse import urljoin

import httpx
from httpx import AsyncClient
from bs4 import BeautifulSoup
import yaml

DOMAIN_URL = "https://djinni.co"
PYTHON_POSITIONS_URL = DOMAIN_URL + "/jobs/?primary_keyword=Python"

with open('config.yml') as c:
    config = yaml.full_load(c)
technologies = config["TECHNOLOGIES"]

junior_technologies = copy.deepcopy(technologies)
middle_technologies = copy.deepcopy(technologies)
senior_technologies = copy.deepcopy(technologies)


def add_time_to_config(time_now):
    with open('config.yml') as f:
        doc = yaml.full_load(f)

    doc['TIME_CREATED'] = time_now

    with open('config.yml', 'w') as f:
        yaml.dump(doc, f)


def write_result(info_technologies: dict) -> None:
    folder_path = os.path.abspath("data_storage")
    time_now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    add_time_to_config(time_now)
    if "years_of_experience" in info_technologies:
        filename = os.path.join(
            folder_path,
            f"{info_technologies['years_of_experience']}_{time_now}.csv"
        )
    else:
        filename = os.path.join(
            folder_path,
            f"all_levels_{time_now}.csv"
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


async def scrape_links_of_positions(page, client: AsyncClient):
    page = await client.get(PYTHON_POSITIONS_URL, params={"page": page})
    soup = BeautifulSoup(page.content, "html.parser")
    return soup


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
