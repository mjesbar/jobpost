from selenium import  webdriver
from selenium.webdriver import Chrome, ChromeOptions


chrome_options = ChromeOptions()
chrome_options.page_load_strategy = 'eager'
chrome_options.add_argument('--start-in-incognito')
chrome_options.add_argument('--headless')
chrome_options.add_argument('--window-position=960,0')
chrome_options.add_argument('--window-size=960,700')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')

browser = webdriver.Chrome(options=chrome_options)
browser.get('https://www.ldoceonline.com/')

print(browser.current_url)
print(browser.title)

browser.quit()



