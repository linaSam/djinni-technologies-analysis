import asyncio
import copy
import csv
import os
import logging
import time
from datetime import datetime
from urllib.parse import urljoin

import httpx
from httpx import AsyncClient
from bs4 import BeautifulSoup
import yaml


logging.basicConfig(level=logging.INFO)

DOMAIN_URL = "https://djinni.co"
PYTHON_POSITIONS_URL = DOMAIN_URL + "/jobs/?primary_keyword=Python"

with open("config.yml") as c:
    config = yaml.full_load(c)
technologies = config["TECHNOLOGIES"]

junior_technologies = copy.deepcopy(technologies)
middle_technologies = copy.deepcopy(technologies)
senior_technologies = copy.deepcopy(technologies)


def add_time_to_config(time_now) -> None:
    with open("config.yml") as file:
        data = yaml.full_load(file)

    data["TIME_CREATED"] = time_now

    with open("config.yml", "w") as file:
        yaml.dump(data, file)


def write_result(technologies_info: dict) -> None:
    folder_path = os.path.abspath("data_storage")
    time_now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    add_time_to_config(time_now)

    if "years_of_experience" in technologies_info:
        filename = os.path.join(
            folder_path, f"{technologies_info['years_of_experience']}_{time_now}.csv"
        )
    else:
        filename = os.path.join(folder_path, f"all_levels_{time_now}.csv")

    with open(filename, "w") as f:
        writer = csv.writer(f)
        for key, value in technologies_info.items():
            if key != "years_of_experience" and value != 0:
                writer.writerow([key, value])


def parse_technologies(soup, some_technologies) -> None:
    if soup.select_one(".profile-page-section") is not None:
        info = soup.select_one(".profile-page-section").text
        for item in technologies:
            if item.lower() in info.lower():
                some_technologies[item] += 1
                technologies[item] += 1


def prepare_file_according_to_experience(index, soup) -> None:
    if index.isdigit():
        if int(index) <= 2:
            junior_technologies["years_of_experience"] = "junior"
            parse_technologies(soup, junior_technologies)
        elif int(index) >= 5:
            senior_technologies["years_of_experience"] = "senior"
            parse_technologies(soup, senior_technologies)
        else:
            middle_technologies["years_of_experience"] = "middle"
            parse_technologies(soup, middle_technologies)
    else:
        junior_technologies["years_of_experience"] = "junior"
        parse_technologies(soup, junior_technologies)


async def define_experience_for_position(
    position_link: str, client: AsyncClient
) -> None:
    link = urljoin(DOMAIN_URL, position_link)
    response = await client.get(link)
    soup = BeautifulSoup(response.content, "html.parser")

    text_with_years = soup.select_one(".job-additional-info").get_text(
        strip=True, separator=" "
    )

    if "?????????? ??????????????" in text_with_years:
        experience_index = text_with_years[text_with_years.find("?????????? ??????????????") - 2]
    elif "???????? ??????????????" in text_with_years:
        experience_index = text_with_years[text_with_years.find("???????? ??????????????") - 2]
    else:
        experience_index = text_with_years[
            (text_with_years.find("??????????????") - 4): (
                text_with_years.find("??????????????") - 1
            )
        ]

    prepare_file_according_to_experience(experience_index, soup)


def get_number_of_pages(page_soup: BeautifulSoup) -> int:
    pagination = page_soup.select_one(".pagination")

    if pagination is None:
        return 1

    return int(pagination.select("a.page-link")[-2].text)


async def get_information_about_position(page_soup: BeautifulSoup) -> None:
    positions = page_soup.select(".profile")
    async with AsyncClient() as client:
        await asyncio.gather(
            *[
                define_experience_for_position(position_link.get("href"), client)
                for position_link in positions
            ]
        )


async def get_links_of_positions_from_page(page, client: AsyncClient) -> BeautifulSoup:
    page = await client.get(PYTHON_POSITIONS_URL, params={"page": page})
    soup = BeautifulSoup(page.content, "html.parser")
    return soup


async def main() -> None:
    logging.info('Process of collecting data started')

    response = httpx.get(PYTHON_POSITIONS_URL)
    first_page_soup = BeautifulSoup(response.content, "html.parser")

    num_pages = get_number_of_pages(first_page_soup)

    await get_information_about_position(first_page_soup)

    async with AsyncClient() as client:
        positions_from_all_pages = await asyncio.gather(
            *[
                get_links_of_positions_from_page(page, client)
                for page in range(2, num_pages + 1)
            ]
        )
        for position in positions_from_all_pages:
            await get_information_about_position(position)
    logging.info('Process of collecting data finished')

    logging.info('Process of writing collected data to files started')
    write_result(junior_technologies)
    write_result(middle_technologies)
    write_result(senior_technologies)
    write_result(technologies)
    logging.info('Files with collected data are ready for analysis')


if __name__ == "__main__":
    start_time = time.perf_counter()
    asyncio.run(main())
    end_time = time.perf_counter()
    print("Elapsed:", end_time - start_time)
