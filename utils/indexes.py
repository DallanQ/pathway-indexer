import re
import os
from typing import Any, cast
import requests
from bs4 import BeautifulSoup, Tag
from playwright.async_api import async_playwright
import time

from utils.tools import create_folder


# clean function for the parse-index
def clean(text: Any) -> str:
    """Convert text to a string and clean it."""
    if text is None:
        return ""
    if isinstance(text, Tag):
        text = text.get_text()
    if not isinstance(text, str):
        text = str(text)
    # Replace non-breaking space with normal space and remove surrounding whitespace.
    text = text.replace(" ", " ").replace("\u200b", "").replace("\u200a", " ")
    text = re.sub(r"(\n\s*)+\n", "\n\n", text)
    text = re.sub(r" +\n", "\n", text)
    text = re.sub(r"\r\n", " ", text)
    return cast(str, text.strip())


class Selectors:
    """Selector for a soup object"""

    def __init__(self, header, sub_header, link, text):
        self.header = header
        self.sub_header = sub_header
        self.link = link
        self.text = text


def get_data(soup: BeautifulSoup, selectors: Selectors) -> list:
    """
    Get the data from the soup object.
    """
    cur_header = None
    cur_sub_header = None
    rows = []  # header, subheader, title, url

    header = selectors.header
    sub_header = selectors.sub_header
    link = selectors.link
    text = selectors.text
    # get the elements inside the div div.WordSection1, independent of the tag
    elems = soup.select("div.WordSection1 > *")
    # elems = soup.select("p.MsoNormal")

    for elem in elems:
        # in this if, vaidate if the element is a header
        if elem.select(sub_header) or elem.name == sub_header:
            if elem.select(sub_header):
                sub_header_text = elem.select(sub_header)[0].text
            else:
                sub_header_text = elem.text
            cur_sub_header = clean(sub_header_text)
        elif elem.select(header) or elem.name == header:
            if elem.select(header):
                header_text = elem.select(header)[0].text
            else:
                header_text = elem.text
            cur_header = clean(header_text)
            cur_sub_header = None
        elif elem.select(link):
            if len(elem.select(link)) > 0:
                link_text = elem.select(link)[0].get_attribute_list("href")[0]
                text_text = (
                    elem.select(text)[0].text
                    if len(elem.select(text))
                    else elem.select(link)[0].text
                )

                # save the row
                rows.append(
                    [cur_header, cur_sub_header, clean(text_text), clean(link_text)]
                )

    return rows


def crawl_index(url, selectors: Selectors):
    """Crawl the index page and get the data."""
    parser = "html.parser"
    response = requests.get(url, timeout=10)
    soup = BeautifulSoup(response.content, features=parser)
    data = get_data(soup, selectors)
    return data


def create_root_folders(root):
    """Create the initial folders for the project."""
    # create the crawl folder and html, others and pdf subfolders
    crawl_folder = os.path.join(root, "crawl")
    create_folder(crawl_folder, is_full=True)
    create_folder(crawl_folder, "html")
    create_folder(crawl_folder, "others")
    create_folder(crawl_folder, "pdf")

    # creta the index folder
    create_folder(root, "index")

    # create the out folder abd from_html, from_pdf, from_others subfolders
    out_folder = os.path.join(root, "out")
    create_folder(out_folder, is_full=True)
    create_folder(out_folder, "from_html")
    create_folder(out_folder, "from_pdf")
    create_folder(out_folder, "from_others")
    create_folder(out_folder, "error")


def get_soup_content(url):
    """Get the soup object from the url."""
    parser = "html.parser"
    response = requests.get(url, timeout=10)
    soup = BeautifulSoup(response.content, features=parser)
    return soup


def get_handbook_data(soup, selector):
    """covert the soup object to a list of lists."""
    data = []
    sections = soup.select(selector, class_="Chapter")

    for section in sections:
        div = section.find("div", class_="Chapter-title")

        # does the div exist?
        if div:
            # if yes, is the first-child a span or an a tag?
            span = div.find("span", class_="Link")
            anchor = div.find("a", class_="Link")

            # name the section depending on the firstchild
            if span:
                section_name = span.text.strip()
            elif anchor:
                section_name = anchor.text.strip()
            else:
                section_name = None
            # print(section_name)
            links = section.find_all("a", class_="Link")
            for link in links:
                title_span = link.find("span", class_="Link-span")
                title = title_span.text.strip() if title_span else ""
                url = link["href"]
                data.append([section_name, title, url])

    return data


async def get_help_links(url, selector):
    """Get the links from the help page."""
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch()
    page = await browser.new_page()

    try:
        await page.goto(url)
        await page.wait_for_load_state()

        time.sleep(2)
        # Get the element with the specified selector
        desktop_articles = await page.query_selector("#desktopArticles")

        if not desktop_articles:
            raise ValueError(f"No element found for selector: {selector}")

        # Get all the <a> tags inside the selected element
        links = await desktop_articles.query_selector_all("a")

        if not links:
            raise ValueError("No links found inside the selected element.")

        # Click "Show More..." until all articles are loaded
        while True:
            try:
                print("Doing Click...")
                await page.click(
                    "#desktopArticles button.show-more-button", timeout=3000
                )
                time.sleep(2)
                # si no aumentó el número de artículos, salir del loop
                new_links_count = await desktop_articles.query_selector_all(
                    "a"
                ) or await desktop_articles.query_selector_all("a")
                if len(links) == len(new_links_count):
                    break
                links = new_links_count
            except Exception as e:
                print(f"Stopped clicking 'Show More...': {e}")
                break

        if not links:
            raise ValueError("No links found inside the selected element.")

        data = []
        for link in links:
            # Get the title (first <p> tag)
            title = await link.query_selector("p.title")
            # Get the description (second <p> tag)
            description = await link.query_selector("p:not(.title)")

            # Validate that title and description exist
            if not title or not description:
                print("Skipping a link with missing title or description.")
                continue

            # Append the data
            data.append(
                [
                    await title.inner_text(),
                    await description.inner_text(),
                    await title.inner_text(),
                    url + (await link.get_attribute("href")),
                ]
            )

        return data
    finally:
        await browser.close()
        await playwright.stop()


async def get_services_links(url):
    """Get the links from the student services page."""
    content = requests.get(url, timeout=10).content
    soup = BeautifulSoup(content, "html.parser")
    # get the nav with aria-label="Navigation"
    nav = soup.find("nav", {"aria-label": "Mobile Navigation"})
    li_elems = nav.find_all("li")
    # save the links and and content
    data = []
    for li in li_elems:
        links = li.find_all("a")
        for link in links:
            # sort by: section, subsection, title, url
            data.append(
                [
                    li.find("span").text,
                    "",
                    link.find("span").text.strip(),
                    url[:-1] + link["href"],
                ]
            )

    return data
