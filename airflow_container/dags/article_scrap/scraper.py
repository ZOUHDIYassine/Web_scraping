
def scraper(link,scraping_data):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    import time

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")

    service = Service('/usr/local/bin/chromedriver-linux64/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)


    # URL of the webpage
    url = link

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



    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    #Title
    title=soup.title.text




    #Adding text
    text=[]
    paragraphs = soup.find_all('div',{'data-component': 'text-block'})
    for paragraph in paragraphs:
        lines= paragraph.find_all('p')
        for line in lines :
            text.append(line.text)
    #Adding images
    images_url=[]
    images = soup.find_all('div',{'data-component': 'image-block'})
    for image in images:
        images_url.append(image.find('div').find('img').get('src'))
    #Author
    author = soup.find('span',{'data-testid':'byline-name'})
    author = author.text.strip() if author else ""
    #Videos extraction
    videos_url=[]
    videos = soup.find_all('div',{'data-component': 'video-block'})

    for video in videos:
        video_element=video.find('iframe')
        if video_element is not None:
            video_link=video_element["src"]
        else:
            video_link = ""
        videos_url.append(video_link)
    
    
    #scraping topics
    topics=[]
    tpcs=soup.find('div',{'data-component':'tags'})
    if tpcs is not None:
        tpcs=tpcs.find_all('a')
        for tpc in tpcs:
            topics.append(tpc.text)

    scraping_data[link]['title']=title
    scraping_data[link]['topics']=topics
    scraping_data[link]['images']=images_url
    scraping_data[link]['videos']=videos_url
    scraping_data[link]['author']=author
    scraping_data[link]['text']=text
    print("Processed page")