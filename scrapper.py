import json, math, time, os
from pyspark import pandas
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions

columns = ['id','title','city','department','type','postDate','company','salary','education','age','experience','description']

#general_dataframe = pandas.DataFrame(columns=columns)
#shift_dataframe = pandas.DataFrame(columns=columns)

# Creating the driver instance ------------------------------------------------------------------
chrome_options = ChromeOptions()
chrome_options.page_load_strategy = 'normal'
chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"')
chrome_options.add_argument('--start-in-incognito')
#chrome_options.add_argument('--headless')
chrome_options.add_argument('--window-position=544,0')
chrome_options.add_argument('--window-size=1376,1080')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')

# going toward the target url
browser = webdriver.Chrome(options=chrome_options)

URL = 'https://co.computrabajo.com/trabajo-de-datos'
browser.get(URL)
print(f"URL     : {browser.current_url}.")
print(f"Site   : {browser.title}.\n")

# setting filters
print(f"Setting filters ... ")
WebDriverWait(browser, timeout=5).until(
    expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "field_select_links.small"))
)
date_filter = browser.find_elements(By.CLASS_NAME, "field_select_links.small")
date_filter[1].click()
date_lastweek = browser.find_element(By.CLASS_NAME, "field_select_links.small.open")
date_lastweek = date_lastweek.find_elements(By.CLASS_NAME, "buildLink")
date_lastweek[1].click()

# getting the total offers found
post_grid = browser.find_element(By.ID, "offersGridOfferContainer")
post_amount = post_grid.find_element(By.TAG_NAME, "span").text
pages_per_page = 20
pages = math.ceil(int(post_amount.replace('.','')) / pages_per_page)
page_index = range(pages_per_page)
print("total amount of offers", post_amount, "Pages", page_index)

# Crawling the url --------------------------------------------------------------------------------
while (True):
    scroll = 0
    # clicking over the annoying suggestion alert
    web_popup = browser.find_element(By.ID, "pop-up-webpush-sub")
    web_popup = web_popup.find_elements(By.TAG_NAME, "button")[0]
    if web_popup.is_displayed():
        web_popup.click()

    # getting the current page main offers element
    post_box = browser.find_element(By.ID, "offersGridOfferContainer")
    posts = post_box.find_elements(By.TAG_NAME, "article")
    for post in posts:
        # selecting the next post
        post.click()
        # waiting for important tags
        WebDriverWait(browser, timeout=5).until(
            expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_offer"))#'post details'
        )
        WebDriverWait(browser, timeout=5).until(
            expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_detail"))#'apply' button, crucial to check intern data is available
        )
        WebDriverWait(browser, timeout=5).until(
            expected_conditions.text_to_be_present_in_element((By.CLASS_NAME, "b_primary.big"), "Postularme")
        )
        # grabbing the data
        post_detail = browser.find_element(By.CLASS_NAME, "box_detail")

        post_id = browser.current_url.split('#')[-1]

        post_title = post_detail.find_element(By.CLASS_NAME, "title_offer.fs21.fwB.lh1_2").text

        place = post_detail.find_element(By.CLASS_NAME, "mb5.mt5.fs16").find_elements(By.TAG_NAME, "span")[-1]
        place = place.text.split(',')
        post_department = place[0] if (place[0]!='Bogotá') else 'Cundinamarca' 
        post_city = place[1] if (place[1]!=' D.C.') else 'Bogotá'

        labels = post_detail.find_elements(By.CLASS_NAME, "tag.base.mb10")
        post_type = labels[-1].text if (('remoto' in labels[-1].text.lower()) | ('presencial' in labels[-1].text.lower())) else None
        post_salary = labels[0].text if ('$' in labels[0].text) else None

        post_date = post_detail.find_element(By.CLASS_NAME, "fc_aux.fs13").text

        post_company = post_detail.find_element(By.CLASS_NAME, "mb5.mt5.fs16")
        try: post_company = post_company.find_element(By.TAG_NAME, "a").text
        except: post_company = None

        post_requirements = post_detail.find_element(By.CLASS_NAME, "fs16.disc.mbB").find_elements(By.TAG_NAME, "li")
        post_education = None
        for item in post_requirements:
            post_education = item.text if ('Educación mínima' in item.text) else None
        
        post_age = None
        for item in post_requirements:
            post_age = item.text if ('Edad' in item.text) else None
        
        post_experience = None
        for item in post_requirements:
            post_experience = item.text if ('experiencia' in item.text) else None

        post_description = None
        fs16class = post_detail.find_elements(By.CLASS_NAME, "fs16")
        for element in fs16class:
            post_description = element.text if (element.get_attribute(name="class") == 'fs16') else None
            if (post_description!=None): break

        print("postinfo:\n", post_description)
        
        # following clicks and scrolling down
        scroll += 140
        browser.execute_script(f"arguments[0].scrollTo(0, {scroll})", post_box)

    # 'next' button
    try:
        WebDriverWait(browser, timeout=5).until(
            expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "b_primary.w48.buildLink.cp")),#'post body'
        )
    except:
        print("that's all bro")
        break
    else:
        # going to the next page
        next_page = browser.find_element(By.CLASS_NAME, "b_primary.w48.buildLink.cp")
        next_page.click()

browser.close()
browser.quit()



