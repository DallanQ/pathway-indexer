import re
import os
from typing import Any, cast
import requests
from bs4 import BeautifulSoup, Tag

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
    #elems = soup.select("p.MsoNormal")

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
            if len(elem.select(link)) > 0 and elem.select(text):
                link_text = elem.select(link)[0].get_attribute_list("href")[0]
                text_text = elem.select(text)[0].text

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
