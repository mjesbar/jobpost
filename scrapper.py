import json, math, time, os, base64, numpy, warnings, pandas, datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions


if (__name__ == "__main__"):
    # configurations
    warnings.filterwarnings('ignore')


    columns = ['id','title','city','department','type','postDate','timeStamp','company','salary','education','age','experience','description']
    partition_date = datetime.date.today()
    partition = pandas.DataFrame(columns=columns)
    shift_data = list()
    null = numpy.nan


    # Creating the driver instance ------------------------------------------------------------------
    chrome_options = ChromeOptions()
    chrome_options.page_load_strategy = 'normal'
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"')
    chrome_options.add_argument('--start-in-incognito')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--window-position=544,0')
    chrome_options.add_argument('--window-size=1376,1080')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-2d-canvas-clip-aa')
    chrome_options.add_argument('--disable-2d-canvas-image-chromium')
    chrome_options.add_argument('--disable-3d-apis')

    # going toward the target url
    browser = webdriver.Chrome(options=chrome_options)

    # setting filters
    print(f"Setting filters ... ")
    datetoday_urlparameter = 'pubdate=1'
    order_urlparameter = 'by=publicationtime'
    root_url = 'https://co.computrabajo.com/trabajo-de-datos'

    # starting the search
    URL = f"{root_url}?{datetoday_urlparameter}&{order_urlparameter}"
    browser.get(URL)

    # getting the total offers found
    post_grid = browser.find_element(By.ID, "offersGridOfferContainer")
    post_amount = post_grid.find_element(By.TAG_NAME, "span").text
    pages_per_page = 20
    pages = math.ceil(int(post_amount.replace('.','')) / pages_per_page)
    page_index = math.ceil(int(post_amount) / pages_per_page)

    # preface information
    print(f"Site : {browser.title}.")
    print(f"URL  : {browser.current_url}.")
    print("Total offers", post_amount)
    print("Pages", page_index)
    page = 1
    graphic = ['·   ','··  ','··· ','····','    ']


    # Crawling the url --------------------------------------------------------------------------------
    while (True):
        
        errors = list()
        scroll = 160
        # clicking over the annoying suggestion alert
        web_popup = browser.find_element(By.ID, "pop-up-webpush-sub")
        web_popup = web_popup.find_elements(By.TAG_NAME, "button")[0]
        if web_popup.is_displayed():
            web_popup.click()

        # getting the current page main offers element
        post_box = browser.find_element(By.ID, "offersGridOfferContainer")
        posts = post_box.find_elements(By.TAG_NAME, "article")

        for (idx, post) in enumerate(posts):
            # rolling graphic
            print(f"\rL Scraping Status page:[{page}/{page_index}] {graphic[idx%5]} ", end='')
            # following clicks and scrolling down
            browser.execute_script(f"arguments[0].scrollTo(0, {scroll*idx})", post_box)

            # selecting the current or next post
            post.click()

            # waiting for important tags
            try:
                WebDriverWait(browser, timeout=10).until(
                    expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_offer")))
                # 'post details'
                WebDriverWait(browser, timeout=10).until(
                    expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "box_detail")))
                # 'apply to' button, crucial to check intern data is available
                WebDriverWait(browser, timeout=10).until(
                    expected_conditions.text_to_be_present_in_element((By.CLASS_NAME, "b_primary.big"), "Postularme"))
            except:
                error_id = browser.current_url.split('#')[-1]
                errors.append(error_id) 
                continue

            # Acquiring generic data about the post
            post_id = browser.current_url.split('#')[-1]
            post_title = post.find_element(By.CLASS_NAME, "js-o-link.fc_base").text
            post_detail = browser.find_element(By.CLASS_NAME, "box_detail")
            post_date = post.find_element(By.CLASS_NAME, "fs13.fc_aux").text
            print(" PostId:", post_id, "\t",end='')
            
            # today alternative filter, only grab the data from today's post
            if ('minuto' not in post_date) & ('hora' not in post_date):
                continue
            time.sleep(0.25)

            # grabbing the inner data
            place = post_detail.find_element(By.CLASS_NAME, "mb5.mt5.fs16").find_elements(By.TAG_NAME, "span")[-1]
            place = place.text.split(',')
            post_department = null
            post_city = null
            if (len(place) > 1):
                post_department = place[0] if (place[0]!='Bogotá') else 'Cundinamarca' 
                post_city = place[1] if (place[1]!=' D.C.') else 'Bogotá'

            labels = post_detail.find_elements(By.CLASS_NAME, "tag.base.mb10")
            post_type = labels[-1].text if (('remoto' in labels[-1].text.lower()) | ('presencial' in labels[-1].text.lower())) else null
            post_salary = labels[0].text if ('$' in labels[0].text) else null

            post_company = post_detail.find_element(By.CLASS_NAME, "mb5.mt5.fs16")
            try: post_company = post_company.find_element(By.TAG_NAME, "a").text
            except: post_company = null

            post_requirements = post_detail.find_element(By.CLASS_NAME, "fs16.disc.mbB").find_elements(By.TAG_NAME, "li")
            post_education = null
            for item in post_requirements:
                post_education = item.text if ('Educación mínima' in item.text) else null
            
            post_age = null
            for item in post_requirements:
                post_age = item.text if ('Edad' in item.text) else null
            
            post_experience = null
            for item in post_requirements:
                post_experience = item.text if ('experiencia' in item.text) else null

            post_description = null
            fs16class = post_detail.find_elements(By.CLASS_NAME, "fs16")
            for element in fs16class:
                if (element.get_attribute("class") == 'fs16'):
                    post_description = element.text
                    break
            post_description_utf8 = str(post_description).encode('utf-8')
            post_description_encoded = base64.b64encode(post_description_utf8)

            timeStamp = partition_date

            # appending the data collected in page
            #['id','title','city','department','type','postDate','timeStamp','company','salary','education','age','experience','description']
            shift_data.append(
                [
                    post_id,
                    post_title,
                    post_city,
                    post_department,
                    post_type,
                    post_date,
                    timeStamp,
                    post_company,
                    post_salary,
                    post_education,
                    post_age,
                    post_experience,
                    post_description_encoded    
                ]
            )
        # } end for

        # diagnostic info
        print(f"[OK] Errors: {len(errors)}. ", end='')
        partition = partition.append(pandas.DataFrame(shift_data, columns=columns),
                                     ignore_index=True)
        print("size:", partition.size, "bytes, shape:", partition.shape, "RowxCol.")
        for (i, error) in enumerate(errors):
            print(f"\t\tL@ Error {i+1}, postId: {error}")

        # saving partition DataFrame
        partition.to_csv(path_or_buf=f'data/batch-jobpost{partition_date}.csv.gz',
                         mode='w',
                         compression={'method': 'gzip'},
                         index=False)
        partition.to_parquet(path=f'data/batch-jobpost{partition_date}.gz.parquet',
                             engine='pyarrow',
                             compression='gzip',
                             index=False)
        # empty the shift_data temporal array
        shift_data.clear()
        del errors

        # clicking the 'next' button
        try:
            WebDriverWait(browser, timeout=10).until(
                expected_conditions.presence_of_all_elements_located((By.CLASS_NAME, "b_primary.w48.buildLink.cp")))
        except:
            print("that's all bro")
            break
        else:
            next_page = browser.find_element(By.CLASS_NAME, "b_primary.w48.buildLink.cp")
            next_page.click()
            page += 1
    # } end while

    browser.close()
    browser.quit()
# } end main



