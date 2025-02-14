import re

# Create a function that will extract the text from the rows along with any links
def hasImage(row):
    return row.find('img') is not None


def extract_text(row):
    # Since there are two images in a row, we need to extract both and put them in a list so we can access them easily
    # The first image is the flag of the country and the second is the stadium image
    images = row.find_all('img')
    # Since some rows have no images of stadium or flag, we have to give them a default value of None
    images = [image['src'] if image else None for image in images]
    cells = row.find_all('td')
    # Rank is in the th tag so we have to extract it separately and add it to the list of cells as the first element
    rank = row.find('th').text.strip()
    cells = [cell.text.strip() for cell in cells]
    cells.insert(0, rank)
    cells.append(images[0])
    # Since the seventh column contains only the image of the stadium
    # The code 'cells = [cell.text.strip() for cell in cells]' ignore it so the value is value is '' so we replace it with the image of the stadium or None
    if len(images) == 2:
        cells[6] = images[1]
    return cells

def clean_cells(text: str):
    # remove the diamond character from the text
    text = text.replace('♦', '')
    # remove [note] from the text
    text = re.sub(r'\[\d+\]', '', text)
    return text
