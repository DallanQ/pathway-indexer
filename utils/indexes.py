import re
import os
import json
from typing import Any, cast, Dict, List, Optional
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


def _clean_json_text(raw_text: str) -> str:
    """
    Remove problematic control characters from raw JSON text
    that could cause json.loads() to fail.
    """
    return re.sub(r'[\x00-\x1f\x7f]', '', raw_text)


def _fetch_help_page(page: int, base_url: str, lang: str = "en", timeout: int = 15) -> Optional[Dict[str, Any]]:
    """
    Fetch a single page of results from the BYU-Pathway Help Center API.
    
    Args:
        page (int): Page number to fetch.
        base_url (str): The base API URL.
        lang (str): Language code for the request.
        timeout (int): Request timeout in seconds.

    Returns:
        dict: Parsed JSON response if successful, else None.
    """
    api_url = f"{base_url.rstrip('/')}/en-US/knowledgebase/fetch-articles/"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ArticleIndexer/1.0)"}
    params = {"page": page, "lang": lang}
    
    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Network error fetching help page {page}: {e}")
        return None

    raw = resp.text
    cleaned = _clean_json_text(raw)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"JSON parse error on help page {page}: {e}")
        return None


def _build_help_article_url(article_id: str, base_url: str, lang: str = "en") -> str:
    """Construct the full article URL from its articleId."""
    return f"{base_url.rstrip('/')}/en-US/knowledgebase/article/?kb={article_id}&lang={lang}"


async def get_help_links(url, selector):
    """
    Get the links from the help page using the API endpoint.
    
    Args:
        url (str): The base help URL (not used directly, but kept for compatibility).
        selector (str): CSS selector (not used with API, but kept for compatibility).
    
    Returns:
        list: List of article data in format [section, subsection, title, url].
    """
    # Extract base URL for API calls
    base_url = "https://help.byupathway.edu"
    if url and url.startswith("http"):
        # Use the provided URL's domain if different
        from urllib.parse import urlparse
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    data = []
    page = 1

    while True:
        page_data = _fetch_help_page(page, base_url)
        if not page_data:
            print(f"Stopping help articles fetch at page {page} due to error.")
            break

        results = page_data.get("results", [])
        for item in results:
            article_id = item.get("articleId")
            title = item.get("title", "")
            
            if article_id and title:
                article_url = _build_help_article_url(article_id, base_url)
                # Format: [section, subsection, title, url]
                # Using "Help Articles" as section and empty subsection for consistency
                data.append(["Help Articles", "", clean(title), article_url])

        # Check if there are more records to fetch
        if not page_data.get("morerecords", False):
            break

        page += 1

    return data


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
