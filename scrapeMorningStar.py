import time
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_equity_research_insights_page(page_number=1):
    """
    Scrapes article listings (e.g., title, URL, author, date) from a single
    page of Morningstar’s 'Equity Research & Insights' listing.

    Returns:
        A list of dicts, each containing metadata about an article:
        [
          {
            "title": <str>,
            "url": <str>,
            "collection": <str>,
            "author": <str>,
            "date": <str>
          },
          ...
        ]
    """

    base_url = (
        "https://www.morningstar.co.uk/uk/collection/2110/2310/"
        "equity-research--insights.aspx"
    )

    params = {"page": page_number}  # e.g. ?page=2
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/108.0.0.0 Safari/537.36"
        )
    }

    # time.sleep(1) # optional: polite delay if scraping many pages quickly

    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Unable to fetch page {page_number}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    # The articles appear in table-like rows; adjust if needed
    table_rows = soup.find_all("tr")
    articles_data = []
    for row in table_rows:
        columns = row.find_all("td")
        # We expect at least 4 columns: title, collection, author, date
        if not columns or len(columns) < 4:
            continue

        title_col = columns[0]
        collection_col = columns[1]
        author_col = columns[2]
        date_col = columns[3]

        link_tag = title_col.find("a", href=True)
        if not link_tag:
            continue

        # Extract metadata
        article_title = link_tag.get_text(strip=True)
        article_url = link_tag["href"]  # might be relative or absolute
        collection_text = collection_col.get_text(strip=True)
        author_text = author_col.get_text(strip=True)
        date_text = date_col.get_text(strip=True)

        articles_data.append({
            "title": article_title,
            "url": article_url,
            "collection": collection_text,
            "author": author_text,
            "date": date_text
        })

    return articles_data

def scrape_article_content(article_url):
    """
    Given a specific article URL, fetches and returns the main text content
    as a single string (or blank if there's an error).

    Adjust the selectors, e.g. searching for main content in <p> tags.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/108.0.0.0 Safari/537.36"
        )
    }

    # time.sleep(1)  # optional: polite delay

    try:
        response = requests.get(article_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Unable to fetch article URL ({article_url}): {e}")
        return ""

    soup = BeautifulSoup(response.text, "html.parser")

    # Grab paragraphs from the main content area
    paragraphs = soup.find_all("p")
    article_text_list = []
    for p in paragraphs:
        text = p.get_text(strip=True)
        if text:
            article_text_list.append(text)

    full_text = "\n\n".join(article_text_list)
    return full_text

def scrape_and_append_to_csv(start_page=3, end_page=500, csv_filename="morningstar_equity_research.csv"):
    """
    Scrapes pages from start_page to end_page. For each page:
      1) Collect article metadata
      2) For each article, fetch content
      3) Immediately convert to a DataFrame
      4) Append the DataFrame to CSV in 'a' (append) mode

    This way, you don't keep all data in memory, and you can resume if needed.
    """

    # Check if CSV already exists to determine whether to write headers or not
    file_already_exists = os.path.isfile(csv_filename)

    for page_num in range(start_page, end_page + 1):
        print(f"Scraping listing on page {page_num}...")
        page_articles = scrape_equity_research_insights_page(page_num)

        # For each article found, fetch the content
        for article in page_articles:
            article_url = article.get("url", "")
            print(f"  Fetching article content: {article_url} ...")
            content = scrape_article_content(article_url)
            article["content"] = content

        # Convert the page's articles to a DataFrame
        df_page = pd.DataFrame(page_articles)

        # If there are no articles or if the page failed, skip CSV writing
        if df_page.empty:
            print(f"  No articles found on page {page_num}.")
            continue

        # Append to CSV. Only include header if file does not already exist
        df_page.to_csv(
            csv_filename,
            mode='a',               # append mode
            index=False,
            encoding='utf-8',
            header=not file_already_exists
        )

        # After the first write, the file definitely has data
        file_already_exists = True

        print(f"  Appended {len(df_page)} article(s) from page {page_num} to {csv_filename}.\n")

def main():
    # Example usage: scrape pages 3 to 10 incrementally
    scrape_and_append_to_csv(
        start_page=200,
        end_page=300,
        csv_filename="morningstar_equity_research2.csv"
    )

if __name__ == "__main__":
    main()