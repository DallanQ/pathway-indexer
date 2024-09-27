import os
import time
import asyncio

import hashlib
import zlib
from playwright.async_api import async_playwright
import requests
import nest_asyncio
import pandas as pd

from utils.tools import create_folder

nest_asyncio.apply()


def generate_content_hash(content):
    """Generate a SHA-256 hash of the content."""
    return hashlib.sha256(content).hexdigest()


def generate_hash_filename(url):
    """Generate a hash of the URL to use as a filename."""
    url_hash = zlib.crc32(url.encode())
    file_name = f"{url_hash:x}"
    return file_name


# whatsapp function
async def get_whatsapp_content(url):
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=False)
    page = await browser.new_page()

    post_xpath = "/html/body/div[1]/div/div/div/div[2]/div/div/div[1]/div[1]/div[2]/div[2]/div/div/div[1]/div/div/div/div/div/div/div"

    print(url)
    await page.goto(url)
    await page.wait_for_load_state()
    post = await page.query_selector(f"xpath={post_xpath}")
    post_content = await post.inner_html()
    await browser.close()
    if post:
        return post_content
    else:
        print(f"Error with {url}")
        return None


async def fetch_content_with_playwright(url, filepath):
    """Fetch the content of a URL using Playwright and save it to a file."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        time.sleep(5)
        content = await page.content()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        await browser.close()


async def crawl_csv(df, base_dir, output_file="output_data.csv"):
    """Takes CSV file in the format Heading, Subheading, Title, URL and processes each URL."""

    # Define a base directory within the user's space
    # base_dir = "../data/data_16_09_24/crawl/"

    # Create directories if they don't exist
    crawl_path = os.path.join(base_dir, "crawl")
    print(crawl_path)
    create_folder(crawl_path, is_full=True)
    create_folder(crawl_path, "html")
    create_folder(crawl_path, "pdf")
    create_folder(crawl_path, "others")

    output_data = []

    async def process_row(row):
        url = row[0]
        heading = row[1]
        sub_heading = row[2]
        title = row[3]
        filename = row[4]

        #! CURRENT EXCEPCION, IGNORE LINKS FROM sharepoint.com
        if "sharepoint.com" in url:
            return

        # Edit the title to become filename

        # Determine the filepaths
        html_filepath = os.path.join(crawl_path, "html", f"{filename}.html")
        pdf_filepath = os.path.join(crawl_path, "pdf", f"{filename}.pdf")

        # Skip fetching if the file already exists
        if os.path.exists(html_filepath) or os.path.exists(pdf_filepath):
            print(f"File already exists for {filename}. Skipping fetch.")
            return

        retry_attempts = 3

        print("Working on ", url)
        while retry_attempts > 0:
            try:
                time.sleep(3)
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # http errors
                content_type = response.headers.get("content-type")

                if any(domain in url for domain in ["faq.whatsapp"]):
                    content = await get_whatsapp_content(url)
                    filepath = html_filepath
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(content)
                    content = content.encode("utf-8")
                elif any(domain in url for domain in ["articulate.com", "myinstitute.churchofjesuschrist.org"]):
                    # raise HTTPError
                    response.status_code = 403

                    raise requests.exceptions.HTTPError
                elif "text/html" in content_type:
                    content = response.text.encode("utf-8")
                    filepath = html_filepath
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(response.text)

                elif "application/pdf" in content_type:
                    content = response.content
                    filepath = pdf_filepath
                    with open(filepath, "wb") as f:
                        f.write(response.content)

                else:
                    # Handle other content types by saving with the correct extension
                    file_extension = content_type.split("/")[-1].split(";")[0]
                    filepath = os.path.join(
                        crawl_path, "others", f"{filename}.{file_extension}"
                    )
                    content = response.content
                    with open(filepath, "wb") as f:
                        f.write(response.content)

                # Create content hash
                content_hash = generate_content_hash(content)

                # Append to the output list
                output_data.append(
                    [
                        heading,
                        sub_heading,
                        title,
                        url,
                        filepath,
                        content_type.split("/")[1].split(";")[0],
                        content_hash,
                    ]
                )
                break  # Exit retry loop after successful fetch

            except requests.exceptions.HTTPError as http_err:
                print(response.status_code)
                if response.status_code == 403:
                    print(
                        f"Access forbidden for {url}: {http_err}. Using Playwright to fetch HTML."
                    )
                    html_filepath = os.path.join(crawl_path, "html", f"{filename}.html")
                    await fetch_content_with_playwright(url, html_filepath)
                    output_data.append(
                        [
                            heading,
                            sub_heading,
                            title,
                            url,
                            html_filepath,
                            "text/html",
                            None,
                        ]
                    )
                    break  # Don't retry if it's a 403 error
                else:
                    print(f"HTTP error occurred for {url}: {http_err}")
                    retry_attempts -= 1
                    if retry_attempts > 0:
                        print("Retrying in 10 seconds...")
                        time.sleep(10)
                    else:
                        output_data.append(
                            [
                                heading,
                                sub_heading,
                                title,
                                url,
                                str(http_err),
                                str(response.status_code),
                                None,
                            ]
                        )

            except requests.exceptions.RequestException as err:
                print(f"Error occurred for {url}: {err}")
                retry_attempts -= 1
                if retry_attempts > 0:
                    print("Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    output_data.append(
                        [heading, sub_heading, title, url, str(err), "Error", None]
                    )

    # Create a list of tasks for asyncio to run
    tasks = [process_row(row) for _, row in df.iterrows()]

    # Run the tasks asynchronously
    await asyncio.gather(*tasks)

    # Create a DataFrame from the output data
    output_df = pd.DataFrame(
        output_data,
        columns=[
            "Heading",
            "Subheading",
            "Title",
            "URL",
            "Filepath",
            "Content Type",
            "Content Hash",
        ],
    )

    out_path = os.path.join(base_dir, output_file)

    # Append to the existing CSV file or create a new one if it doesn't exist
    if os.path.exists(out_path):
        output_df.to_csv(out_path, mode="w", header=False, index=False)
    else:
        output_df.to_csv(out_path, index=False)

    print(f"Processing completed. Output saved to {out_path}")
