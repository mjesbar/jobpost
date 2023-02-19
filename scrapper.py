import json, time, os
from re import A
from selenium import webdriver
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions



# Creating the driver instance ------------------------------------------------------------------
chrome_options = ChromeOptions()
chrome_options.page_load_strategy = 'normal'
chrome_options.add_argument('--start-in-incognito')
#Chrome_options.add_argument('--headless')
chrome_options.add_argument('--window-position=0,0')
chrome_options.add_argument('--window-size=1376,1080')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')

# going toward the target url
browser = webdriver.Chrome(options=chrome_options)
browser.get('https://co.computrabajo.com/trabajo-de-datos')
print(f"URL: {browser.current_url}, Title: {browser.title}\n")

# setting filters
print(f"Setting filters ... ")
WebDriverWait(browser, timeout=5).until(
    expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "field_select_links.small"))
)
date_filter = browser.find_elements(By.CLASS_NAME, "field_select_links.small")
date_filter[1].click()
date_lastmonth = browser.find_element(By.CLASS_NAME, "field_select_links.small.open")
date_lastmonth = date_lastmonth.find_elements(By.CLASS_NAME, "buildLink")
date_lastmonth[-1].click()
# clicking over the annoying suggestion alert
web_popup = browser.find_element(By.ID, "pop-up-webpush-sub")
web_popup = web_popup.find_elements(By.TAG_NAME, "div")[1]
web_popup = web_popup.find_element(By.TAG_NAME, "button")
if web_popup.is_displayed():
    web_popup.click()

# getting the current page main offers element
posts = browser.find_elements(By.CLASS_NAME, "box_offer")


# Crawling the url --------------------------------------------------------------------------------
for post in posts[0:5]:
    # selecting the next post
    post.click()
    # waiting for post details, nextpage button and all the post main tags
    WebDriverWait(browser, timeout=5).until(
        expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "b_primary.w48.buildLink.cp"))
    )
    WebDriverWait(browser, timeout=5).until(
        expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_offer"))
    )
    WebDriverWait(browser, timeout=5).until(
        expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_detail"))
    )
    WebDriverWait(browser, timeout=5).until(
        expected_conditions.text_to_be_present_in_element((By.CLASS_NAME, "b_primary.big"), "Postularme")
    )
    post_detail = browser.find_element(By.CLASS_NAME, "box_detail")
    post_title  = post_detail.find_element(By.CLASS_NAME, "title_offer.fs21.fwB.lh1_2")
    post_salary = post_detail.find_element(By.CLASS_NAME, "tag.base.mb10")
    print("title:   ", post_title.text)
    print("salary:  ", post_salary.text)

browser.close()
browser.quit()



