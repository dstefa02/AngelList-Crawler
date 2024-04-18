###############################################################################
### Libraries ###
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from os import getcwd
from pprint import pprint
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime
from pyvirtualdisplay import Display
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import os, sys, datetime, re, time, json, traceback, random, logging, pickle, requests
from pymongo import MongoClient
from pyvirtualdisplay import Display
import config_angellist as config
###############################################################################

###############################################################################
### Global Variables ###
# KAFKA Parameters
KAFKA_SERVER = "10.166.0.2:9095"
KAFKA_TOPIC_QUERIES = "queries_topic"
KAFKA_TOPIC_RESULTS = "results_topic"

# The url of the website for AngelList login
ANGELLIST_LOGIN_URL = 'https://angel.co/login'

# Cookies dir
CHROME_COOKIE_FILE = "cookies/chrome_cookie.pkl"
FIREFOX_COOKIE_FILE = "cookies/firefox_cookie.pkl"

# Stored browser/drivers of selenium
browsers = []
###############################################################################
def get_browser(browser_name='Firefox'):
    # Selenium options for headless browser
    options = Options()
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')

    # The browser (firefox or chrome) that Selenium will use
    if browser_name == 'Firefox':
        browser = webdriver.Remote(command_executor='127.0.0.1:4444/wd/hub', 
            desired_capabilities=DesiredCapabilities.FIREFOX, options=options)
    elif browser_name == 'Chrome':
        browser = webdriver.Remote(command_executor='127.0.0.1:5555/wd/hub', 
            desired_capabilities=DesiredCapabilities.CHROME, options=options)

    browsers.append(browser)

    return browser
###############################################################################
def get_cookies(browser, cookie_filename):
    # Login to AngelList
    browser.get(ANGELLIST_LOGIN_URL)
    time.sleep(3)

    # Check if cookies already exist
    if os.path.isfile(cookie_filename):
        print('Use existing cookies...', cookie_filename)
        cookies = pickle.load(open(cookie_filename, 'rb'))
        for cookie in cookies:
            if 'expiry' in cookie:
                cookie['expiry'] = int(str(cookie['expiry']).split('.', 1)[0])
                # del cookie['expiry']
            browser.add_cookie(cookie)
    else:
        print('Get cookies...', cookie_filename)
        time.sleep(random.uniform(3, 8))

        # Set email
        browser.find_element_by_id('user_email').send_keys(config.email)
        time.sleep(random.uniform(2, 5))
        
        # Set password
        browser.find_element_by_id('user_password').send_keys(config.password)
        time.sleep(random.uniform(2, 5))
        
        # Press submit button
        browser.find_element_by_name('commit').click()
        time.sleep(random.uniform(5, 10))

        # Get cookies from browser and store them to a pickle
        pickle.dump(browser.get_cookies(), open(cookie_filename,'wb'))

    print('Done\n')
###############################################################################
def clean_queue(consumer):
    print('\nClean queue...')
    # consume and clean messages from topic 'results'
    for msg in consumer:
        consumer.commit()
    print('Done\n')
###############################################################################
def random_crawling_pattern(firefox_browser, chrome_browser, total_companies):
    # Random user-agent
    random_browser = random.randint(0,1)
    if random_browser == 0:
        browser = chrome_browser
        print('> Using CHROME')
    else:
        browser = firefox_browser
        print('> Using FIREFOX')

    # Actions for move cursor
    actions = ActionChains(browser)

    # Scroll randomly
    random_scroll = random.randint(0,4)
    if random_scroll == 0:
        # go pg down: Keys.PAGE_DOWN
        browser.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
        time.sleep(2)
        # go all up: Keys.HOME
        browser.find_element_by_tag_name('body').send_keys(Keys.HOME)
    elif random_scroll == 1:
        # go pg down: Keys.PAGE_DOWN
        browser.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
    elif random_scroll == 2:
        # go pg down: Keys.PAGE_DOWN
        browser.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
        time.sleep(1)
        # go pg down: Keys.PAGE_DOWN
        browser.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
        time.sleep(1)
        # go pg up: Keys.PAGE_UP
        browser.find_element_by_tag_name('body').send_keys(Keys.PAGE_UP)
    elif random_scroll == 3:
        # go all down: Keys.END
        browser.find_element_by_tag_name('body').send_keys(Keys.END)
    elif random_scroll == 4:
        # do nothing
        pass
    time.sleep(2)


    # Go up at the top - move-slowly cursor in order to change website
    try:
        for i in range(0, 3):
            actions.move_by_offset(20, 20).perform()
            time.sleep(0.2)
    except: # MoveTargetOutOfBoundsException: Message: (0, 850) is out of bounds of viewport
        # print('         ', 'ERROR move_by_offset: 20, 20')
        pass
    try:
        for i in range(0, 3):
            actions.move_by_offset(-20, -20).perform()
            time.sleep(0.2)
    except: # MoveTargetOutOfBoundsException: Message: (0, 850) is out of bounds of viewport
        # print('         ', 'ERROR move_by_offset: -5, -5')
        pass

    # Simulate random Right-Click

    # # Click Random link
    # random_scroll = random.randint(0,1)
    # if random_scroll == 0:
    #     # Find all links
    #     list_random_links = browser.find_elements_by_tag_name("a")
    #     print('list_random_links:', len(list_random_links))
    #     list_random_links_v2 = []
    #     for i in range(0, len(list_random_links)):
    #         if 'https://angel.co/' in list_random_links[i].get_attribute('href'):
    #             list_random_links_v2.append(list_random_links[i])

    #     print('list_random_links_v2:', len(list_random_links_v2))
    #     # Select random link
    #     random_link = list_random_links_v2[random.randint(0, len(list_random_links_v2))]
    #     # Move cursor to random link
    #     actions.move_to_element(random_link).perform()
    #     time.sleep(1)
    #     # Click the random link
    #     random_link.click()
    #     time.sleep(3)

    # Sleep for random time
    # If we used at least 2 scrolls for parsing then wait less time
    if total_companies >= 40: 
        time.sleep(random.randint(15,30))
    else:
        time.sleep(random.randint(25, 30))
    # time.sleep(random.randint(15,30))

    return browser
###############################################################################
def slave_start():    
    print('Start: slave_crawler')
    print('='*50)

    # Initialize Kafka producer and consumer
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, 
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="earliest", consumer_timeout_ms=1000, 
        enable_auto_commit=True, group_id='qry-1', max_poll_interval_ms=3000000,
        partition_assignment_strategy=[RoundRobinPartitionAssignor])
    consumer.subscribe(KAFKA_TOPIC_QUERIES)
    clean_queue(consumer)

    try:
        # Initialize browser
        firefox_browser = get_browser(browser_name='Firefox')
        chrome_browser = get_browser(browser_name='Chrome')

        # Get cookies for AngelList account
        get_cookies(firefox_browser, FIREFOX_COOKIE_FILE)
        get_cookies(chrome_browser, CHROME_COOKIE_FILE)

        num_errors = 0
        browser = firefox_browser
        print('Start consuming...')
        print('='*50)
        while True:
            for msg in consumer:

                print('Query: ' + str(msg.value))
                crawled_data = msg.value
                crawled_data['crawled_orgs'] = {}
                consumer.commit()

                if msg.value['stage'] == '1':
                    total_companies = -1
                    total_companies, crawled_orgs = first_phase_crawler(browser, msg.value['url'])
                    crawled_data['crawled_orgs'] = crawled_orgs
                    crawled_data['total_companies'] = total_companies

                    # If blacklisted
                    if total_companies == -1:
                        num_errors += 1
                        # push back to queue 'queries' because of error
                        producer.send(KAFKA_TOPIC_QUERIES, value=msg.value)

                        # Close consumer with error message
                        if num_errors == 3:
                            print('='*50)
                            print('EXIT...3 errors')
                            exit()
                    else:
                        num_errors = 0
                        producer.send(KAFKA_TOPIC_RESULTS, value=crawled_data)
                        print('-'*70)

                    # Insert randomness to crawling
                    browser = random_crawling_pattern(firefox_browser, chrome_browser, total_companies)

                elif msg.value['stage'] == '2':
                    second_phase_crawler(browser, msg.value['url'])

    finally:
        print('\nCLOSE ALL BROWSERS...WAIT!')
        for browser in browsers:
            browser.quit()
        print('Done\n')


    print('='*50)
    print('Finish: slave_crawler')
###############################################################################
def first_phase_crawler(browser, url):
    print('Waiting for crawling...')

    # Actions for move cursor
    actions = ActionChains(browser)

    # Get total companies for crawling
    total_companies = get_orgs_number(browser, url)
    
    # Dictionary that contains the crawled companies
    crawled_orgs = dict()

    # If less than 400 then crawl the companies
    if total_companies > 0 and total_companies <= 400:

        # Continue getting companies by pressing "more" button/item
        companies_crawled = 0
        errors_count = 0
        time.sleep(5)
        while(companies_crawled < total_companies):

            count_new_companies, crawled_orgs = parse_results(crawled_orgs, browser)
            companies_crawled += count_new_companies
            print('         ' + datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 
                ', Count_new_companies:', str(count_new_companies), ', Companies_crawled:', str(companies_crawled))

            try:
                # Scroll at the end
                browser.find_element_by_tag_name('body').send_keys(Keys.END)
                time.sleep(1)

                # Move curson to 'more' button
                more_button = browser.find_element_by_class_name('more')
                try:
                    actions.move_to_element(more_button).perform()
                    time.sleep(2)
                except: # MoveTargetOutOfBoundsException: Message: (0, 850) is out of bounds of viewport
                    pass

                # Press button 'more' to get the next items
                more_button.click()
                
                # browser.find_element_by_class_name('more').click()
                time.sleep(random.randint(5,10))
            except:
                error_str = str(traceback.format_exc())

                if errors_count == 3:
                    print('         ', 'ERROR: could not retrieved all companies!')
                    exit()

                if ('Unable to locate element: .more' in error_str or 
                    ' Unable to locate element: {"method":"css selector","selector":".more"}' in error_str):
                    
                    if companies_crawled < total_companies-1:
                        # Refresh page
                        browser.get(url)
                        time.sleep(10)
                        print('         ', 'Waiting: not yet retrieved all companies!...refresh page')
                        errors_count += 1
                    elif companies_crawled == total_companies-1:
                        break
                else:
                    print(error_str)
                    exit()

    
    return total_companies, crawled_orgs
###############################################################################
def get_orgs_number(browser, url):
    # Get total companies for crawling
    num_errors = 0
    flag = False
    total_companies = -1

    # Get initial website of AngelList
    browser.get(url)
    time.sleep(5)

    while num_errors < 3 and flag == False:
        try:
            class_count = browser.find_element_by_xpath("//div[contains(@class, 'count')]") 
            total_companies = int(class_count.text.replace(',', '').replace(' Companies', '').replace(' Company', ''))
            flag = True
            print('     ', 'Total companies: ' + str(total_companies))
        except:
            print(traceback.format_exc())
            print('     ', 'Class <count> does not exist')
            num_errors += 1
            time.sleep(num_errors*3)

    return total_companies
###############################################################################
def parse_results(crawled_orgs, browser):

    html_source = browser.page_source
    sourcedata = html_source.encode('utf-8')
    soup = BeautifulSoup(sourcedata, 'html.parser')
    all_divs_companies = soup.body.findAll('div', {'data-_tn': 'companies/row'})

    count_new_companies = 0
    for company_row in all_divs_companies:

        # The object for storing in MongoDB
        company_fields = {
            'name': '',
            'pitch': '',
            'angel_link': '',
            'joined_at': '',
            'website': '',
            'location': '',
            'market': '',
            'size': '',
            'stage': '',
            'total_raised': ''
        }

        try:
            # Company column
            company_col = company_row.select('div.company.column')
            company_col_name = company_col[0].select('div.name')
            company_col_name_href = company_col_name[0].select('a.startup-link')
            company_fields['name'] = company_col_name_href[0].text
            company_fields['angel_link'] = company_col_name_href[0]['href']
            company_fields['pitch'] = company_col[0].select('div.pitch')[0].text.strip()
        except:
            # error if first row is the headers
            continue
        
        # Joined column
        company_joined = company_row.select('div.column.joined')
        if len(company_joined) >= 1:
            company_fields['joined_at'] = company_joined[0].select('div.value')[0].text.strip()

        # Location column
        company_location = company_row.select('div.column.location')
        company_location_tag = company_location[0].select('div.tag')
        if len(company_location_tag) == 1:
            company_fields['location'] = company_location_tag[0].text

        # Market column
        company_market = company_row.select('div.column.market')
        company_market_tag = company_market[0].select('div.tag')
        if len(company_market_tag) == 1:
            company_fields['market'] = company_market_tag[0].text

        # Website column
        company_website = company_row.select('div.column.website')
        if len(company_website) >= 1:
            try:
                company_fields['website'] = company_website[0].select('a')[0]['href']
            except:
                # print('         ', 'Something wrong with website:', str(company_website[0]))
                pass

        # Employees column
        company_size = company_row.select('div.column.company_size')
        company_fields['size'] = company_size[0].select('div.value')[0].text.strip()
        if len(company_fields['size']) == '-':
            company_fields['size'] = ''

        # Stage column
        company_stage = company_row.select('div.column.stage')
        if len(company_stage) >= 1:
            company_fields['stage'] = company_stage[0].select('div.value')[0].text
            company_fields['stage'] = company_fields['stage'].strip().replace('-', '')

        # Total raised column
        company_raised = company_row.select('div.column.raised')
        if len(company_raised) >= 1:
            company_fields['total_raised'] = company_raised[0].select('div.value')[0].text
            company_fields['total_raised'] = company_fields['total_raised'].strip().replace(',','').replace('-', '')

        if not company_fields['name'] in crawled_orgs:
            crawled_orgs[company_fields['name']] = company_fields
            count_new_companies += 1

    return count_new_companies, crawled_orgs
###############################################################################
def second_phase_crawler(browser):
    pass
###############################################################################

if __name__ == "__main__":

    slave_start()
