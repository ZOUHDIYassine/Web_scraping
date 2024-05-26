import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from time_parser.parser import parse_time_ago
from article_scrap.scraper import scraper
import json
from time_encoder.encoder import DateTimeEncoder

# Path to your WebDriver (make sure to specify the correct path)
#webdriver_path = 'C:/Users/yassine/Downloads/chromedriver-win64/chromedriver.exe'  

# Initialize the WebDriver
service = Service(r'C:\Users\yassine\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe')
driver = webdriver.Chrome(service=service)

# URL of the webpage
url ="https://www.bbc.com/news"

# Fetch the webpage
driver.get(url)

# Wait for the page to load (you might need to adjust the time depending on your internet speed and the complexity of the page)
time.sleep(5)

    # Alternatively, wait for a specific element to be present
try:
    element_present = EC.presence_of_element_located((By.TAG_NAME, 'article'))
    WebDriverWait(driver, 10).until(element_present)
except:
    print("Timeout waiting for page to load")

# Get the HTML content after JavaScript execution
html_content = driver.page_source

# Close the browser
driver.quit()


scraping_data={}



# Parse the HTML content of the webpage
soup = BeautifulSoup(html_content, 'html.parser')

#scrapping menu
menu=[]
menu_cont=soup.find('nav',{'data-testid':'level1-navigation-container'})
menu_cont=menu_cont.find_all('a')
for elm in menu_cont:
    menu.append(elm.text)
print(menu)

#scrapping submenu
submenu=[]
submenu_cont=soup.find('nav',{'data-testid':'level2-navigation-container'})
menu_cont=submenu_cont.find_all('a')
for elm in menu_cont:
    submenu.append(elm.text)
print(submenu)





matching_tags=[]
# Find all article elements on the page
article = soup.find("article")
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
    #Extarcting link
    link=get_internal_link(tag)
    if link is not None:
        link='https://www.bbc.com'+link
    #link=tag.find('a', {'data-testid': 'internal-link'})["href"]
    #link = link.text.strip() if link else ""
    #Extracting subtitle
    subtitle_element=tag.find('p', {'data-testid': 'card-description'})
    subtitle = subtitle_element.text.strip() if subtitle_element else ""

    #Extracting time_pose
    time_ago_element= tag.find('span',{'data-testid':'card-metadata-lastupdated'})
    time_ago = time_ago_element.text.strip() if time_ago_element else ""
    time_post=parse_time_ago(time_ago)
    #print(time_post)
    

    if link is not None:
        scraping_data[link]={'link':link,'subtitle':subtitle,"time":time_post}
print(scraping_data)

for link in scraping_data.keys():
    scraper(link,scraping_data)

#print(scraping_data)



# Writing dictionary to a JSON file
with open('scraping_news_data.json', 'w') as json_file:
    # Serialize scraped data to JSON using the custom encoder and store it in json_file
    json.dump(scraping_data, json_file, cls=DateTimeEncoder)




