import requests
from bs4 import BeautifulSoup as bs
from scraping_utils import hasImage, extract_text, clean_cells


def scrape():
    website = 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'

    response = requests.get(website)

    soup = bs(response.text, 'html.parser')

    table = soup.find('table', {'class': 'wikitable sortable sticky-header'})
    rows = table.find_all('tr')
    headers = rows[0].find_all('th')
    headers = [header.text.strip() for header in headers]
    headers.append('Flag')

    # count_true = 0
    # for row in rows:
    #     if hasImage(row):
    #         count_true += 1
    # after the loop, count_true should be equal to 382
    data = []
    for row in rows:
        cells = []
        if hasImage(row):
            cells = extract_text(row)
        else:
            cells = [cell.text.strip() for cell in row.find_all('td')]
        cells = [clean_cells(cell) for cell in cells]
        data.append(cells)

    data = data[1:]

    # change the column names to lowercase with underscores
    headers = [header.lower().replace(' ', '_') for header in headers]
    return data
