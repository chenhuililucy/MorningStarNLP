# MorningStarNLP
`scrapeMorningStar.py`
This Python script scrapes article listings and full article content from the Equity Research & Insights section of the Morningstar UK website. The script retrieves metadata such as title, URL, author, and publication date, and then fetches the full article content. The data is saved to a CSV file for further analysis.
Run the scraper for a specified page range. The default configuration scrapes pages 200 to 300.
Usage:

Run the script using:
```python
python script.py
```
This will scrape pages 200 to 300 and save the data to morningstar_equity_research2.csv.

To modify the page range, update the start_page and end_page values in the main() function.
