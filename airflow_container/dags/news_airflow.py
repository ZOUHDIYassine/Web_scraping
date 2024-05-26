from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Your web scraping function
def web_scraping():
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
    from selenium.webdriver.chrome.options import Options

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")

    service = Service('/usr/local/bin/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)


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
    menu_cont=soup.find('nav')
    menu_cont=menu_cont.find_all('a')
    for elm in menu_cont:
        menu.append(elm.text)

    #scrapping submenu
    #submenu=[]
    #submenu_cont=soup.find('nav',{'data-testid':'level2-navigation-container'})
    #submenu_cont=submenu_cont.find_all('a')
    #for elm in submenu_cont:
    #    submenu.append(elm.text)





    matching_tags=[]
    # Find all article elements on the page
    article = soup.find("article")
    tags = article.find_all('div', attrs={'data-testid': True})
        
    # Filter tags whose data-testid ends with '-card'
    for tag in tags:
        if tag.get('data-testid', '').endswith('-card'):
            matching_tags.append(tag)
    #function
    def get_internal_link(tag):
        link_tag = tag.find('a', {'data-testid': 'internal-link'})
        if link_tag is not None:
            return link_tag.get("href")


    for tag in matching_tags:
        link=get_internal_link(tag)
        if link is not None:
            link='https://www.bbc.com'+link
        #link=tag.find('a', {'data-testid': 'internal-link'})["href"]
        #link = link.text.strip() if link else ""
        subtitle_element=tag.find('p', {'data-testid': 'card-description'})
        subtitle = subtitle_element.text.strip() if subtitle_element else ""

        #hnaya nzid
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


# Default arguments for the DAG
start_date = datetime(2024, 5, 6)
end_date = start_date + timedelta(days=5)  # 5 days later

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'end_date': end_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'news_scraping_dag',
    default_args=default_args,
    description='A simple web scraping DAG',
    schedule=timedelta(days=1),
)

# Define the task
run_web_scraping = PythonOperator(
    task_id='run_web_scraping',
    python_callable=web_scraping,
    dag=dag,
)

# Set the task in the DAG
run_web_scraping