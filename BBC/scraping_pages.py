import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
from time_parser.parser import parse_time_ago
from article_scrap.scraper import scraper
from time_encoder.encoder import DateTimeEncoder

# Path to your WebDriver (make sure to specify the correct path)
service = Service(r'C:\Users\yassine\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe')
driver = webdriver.Chrome(service=service)

# URL of the webpage
url = "https://www.bbc.com/future-planet"
driver.get(url)

# Counter to keep track of page numbers
page_counter = 1

# Function to scrape data from a single page
def scrape_page(soup, page_num):
    scraping_data = {}
    # Scrape menu
    menu = []
    menu_cont = soup.find('nav', {'data-testid': 'level1-navigation-container'})
    if menu_cont:
        menu_cont = menu_cont.find_all('a')
        for elm in menu_cont:
            menu.append(elm.text)
    print(menu)

    # Scrape submenu
    submenu = []
    submenu_cont = soup.find('nav', {'data-testid': 'level2-navigation-container'})
    if submenu_cont:
        menu_cont = submenu_cont.find_all('a')
        for elm in menu_cont:
            submenu.append(elm.text)
    print(submenu)

    matching_tags = []
    # Find all article elements on the page
    article = soup.find("article")
    article = article.find('section', {'data-testid': 'alaska-section-outer'})
    if article:
        tags = article.find_all('div', attrs={'data-testid': True})
        # Filter tags whose data-testid ends with '-card'
        for tag in tags:
            if tag.get('data-testid', '').endswith('-card'):
                matching_tags.append(tag)

    # Function to get article link
    def get_internal_link(tag):
        link_tag = tag.find('a', {'data-testid': 'internal-link'})
        if link_tag is not None:
            return link_tag.get("href")

    for tag in matching_tags:
        # Extracting link
        link = get_internal_link(tag)
        if link is not None:
            link = 'https://www.bbc.com' + link
        # Extracting subtitle
        subtitle_element = tag.find('p', {'data-testid': 'card-description'})
        subtitle = subtitle_element.text.strip() if subtitle_element else ""

        # Extracting time_pose
        time_ago_element = tag.find('span', {'data-testid': 'card-metadata-lastupdated'})
        time_ago = time_ago_element.text.strip() if time_ago_element else ""
        time_post = parse_time_ago(time_ago)

        if link is not None:
            scraping_data[link] = {'link': link, 'subtitle': subtitle, "time": time_post}
            scraper(link, scraping_data)

    # Write the data to a new JSON file for this page
    write_data_to_json(scraping_data, page_num)

# Function to navigate to the next page
def go_to_next_page(driver):
    try:
        # Adjusted locator for the "Next" button
        next_button_locator = (By.CSS_SELECTOR, 'button[data-testid="pagination-next-button"]')
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(next_button_locator)
        )
        next_button.click()
        return True
    except:
        return False

# Function to write data to a new JSON file
def write_data_to_json(data, page_num):
    file_path = f'earth_data_page_{page_num}.json'
    with open(file_path, 'w') as file:
        json.dump(data, file, cls=DateTimeEncoder, indent=4)

# Initialize scraping
while True:
    # Wait for the page to load
    time.sleep(5)
    try:
        element_present = EC.presence_of_element_located((By.TAG_NAME, 'article'))
        WebDriverWait(driver, 10).until(element_present)
    except:
        print("Timeout waiting for page to load")
        break

    # Get the HTML content after JavaScript execution
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Scrape the current page
    scrape_page(soup, page_counter)
    
    # Check if there is a next page, if not, break the loop
    if not go_to_next_page(driver):
        break

    # Increment the page counter
    page_counter += 1

# Close the browser
driver.quit()
