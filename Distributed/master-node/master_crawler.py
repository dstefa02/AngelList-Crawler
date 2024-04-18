###############################################################################
### Libraries ###
from kafka import KafkaConsumer, KafkaProducer
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
from selenium.webdriver.support import expected_conditions as EC
import os, sys, datetime, re, time, json, traceback, random, logging, pickle, requests
from pymongo import MongoClient
from pyvirtualdisplay import Display
###############################################################################

###############################################################################
### Global Variables ###
# KAFKA Parameters
KAFKA_SERVER = "10.166.0.2:9095"
KAFKA_TOPIC_QUERIES = "queries_topic"
KAFKA_TOPIC_RESULTS = "results_topic"

# The url for crawling all locations from AngelList
ANGELLIST_LOCATIONS_URL = 'https://angel.co/locations'
# The url for crawling all markets from AngelList
ANGELLIST_MARKETS_URL = 'https://angel.co/markets'

# The url of the website for AngelList login
ANGELLIST_LOGIN_URL = 'https://angel.co/login'
# The url of the website for crawling the AngelList
ANGELLIST_BASIC_URL = 'https://angel.co/companies'
# The url of the website for crawling the type=startups AngelList
ANGELLIST_STARTUPS_URL = 'https://angel.co/companies?company_types[]=Startup'

# Database ip
DATABASE_IP = 'localhost'
# Database port
DATABASE_PORT = 27015
# Database name for storing AngelList
DATABASE_NAME = 'AngelList_01_2020'
# Collection name for storing AngelList organizations
DATABASE_ORGS_COLLECTION = 'organizations'
# Collection name for storing AngelList successful queries
DATABASE_QUERIES_COLLECTION = 'queries_stage_1'
# Collection name for storing AngelList organizations
# DATABASE_PEOPLE_COLLECTION = 'people'
# DATABASE_FUNDING_ROUNDS_COLLECTION = 'funding_rounds'
# DATABASE_RELAT_PEOPLE_COLLECTION = 'Relationships_people'
# DATABASE_RELAT_ORGS_COLLECTION = 'Relationships_organizations'

# Data filenames for pickles
PICKLE_LOCATIONS = 'pickles/locations_dict.pickle'
PICKLE_MARKETS = 'pickles/markets_dict.pickle'

# Cookies dir
COOKIE_FILE = "cookies/cookie.pkl"

# Stored browser/drivers of selenium
browsers = []
# Successful queries
success_queries = dict()
# Current Layer for queries
CUR_LAYER = 1
###############################################################################
def get_browser():
    # Selenium options for headless browser
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    # The browser (firefox or chrome) that Selenium will use
    browser = webdriver.Remote(command_executor='127.0.0.1:4444/wd/hub', desired_capabilities=DesiredCapabilities.FIREFOX, options=options)

    browsers.append(browser)

    return browser
###############################################################################
def get_cookies(browser, cookie_filename):
    # Login to AngelList
    browser.get(ANGELLIST_LOGIN_URL)
    time.sleep(3)

    # Check if cookies already exist
    if os.path.isfile(cookie_filename):
        print('     Use existing cookies...')
        cookies = pickle.load(open(cookie_filename, 'rb'))
        for cookie in cookies:
            browser.add_cookie(cookie)
    else:
        print('     Get cookies...')
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
###############################################################################
def download_all_locations():
    print('\nStart: Crawl and store all locations from AngelList')
    print('='*70)
    sys.stdout.flush()

    locations_dict = dict()
    locations_by_id_dict = dict()
    uniq_locations = dict()

    # If file exist and size bigger than 1MB then return
    if os.path.exists(PICKLE_LOCATIONS) and os.path.getsize(PICKLE_LOCATIONS)/(1024) > 1:
        print('    ALREADY EXIST:', os.path.exists(PICKLE_LOCATIONS))
        print('   ', round(os.path.getsize(PICKLE_LOCATIONS)/(1024),2), 'KB')

        # Load data (deserialize)
        pickle_handle = open(PICKLE_LOCATIONS, 'rb')
        locations_dict = pickle.load(pickle_handle)
    else:
        print('\nStart Crawling...')
        # Get selenium instance
        browser = get_browser()

        # Get initial website of AngelList
        browser.get(ANGELLIST_LOCATIONS_URL)
        time.sleep(3)

        new_items = 1

        while new_items > 0:

            new_items = 0
            time.sleep(2)

            print('\n')
            print('+'*40, '\n', 'Total items for iteration: ', len(browser.find_elements_by_class_name('items')))
            print('+'*40, '\n')

            for item in browser.find_elements_by_class_name('items'):

                if item.text == 'More':
                    item.click()
                    print('CLICK <more>')
                    print(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
                    print('-'*40)
                    # time.sleep(1)
                    continue

                html_source = item.get_attribute('outerHTML')
                sourcedata = html_source.encode('utf-8')
                soup = BeautifulSoup(sourcedata, 'html.parser')

                try:
                    item_tag = soup.find('div', {'class': 'item-tag'})
                    item_name = item_tag.text.replace('\n', '')
                    item_href = item_tag.find('a')['href']
                    item_stats_startups = soup.find('div', {'class': 'item-startups'})

                    res1 = soup.find('div', {'class': 'collapsed'})
                    res2 = soup.find('div', {'class': 'loaded'})

                    if item_name == 'Location' or item_name in uniq_locations or res1 != None or res2 != None:
                        continue
                except:
                    continue

                item_total_jobs = int(soup.find('div', {'class': 'item-jobs'}).find('a').text.replace(',', ''))
                item_total_followers = int(soup.find('div', {'class': 'item-followers'}).find('a').text.replace(',', ''))
                item_total_investors = int(soup.find('div', {'class': 'item-investors'}).find('a').text.replace(',', ''))
                item_total_startups = int(soup.find('div', {'class': 'item-startups'}).find('a').text.replace(',', ''))
                item_data_tag_id = soup.find('div', {'class': 'items'})['data-tag_id']

                parent_data_tag_id = '' # change it to zero for zero node
                parent_div = item.find_element_by_xpath('../../..')
                parent_classes = parent_div.get_attribute('class')

                if 'more_items' in parent_classes and 'core_load_request' in parent_classes: 
                    parent_data_tag_id = parent_div.get_attribute('data-tag_id')
                else:
                    parent_div = item.find_element_by_xpath('../../../..')
                    parent_classes = parent_div.get_attribute('class')
                    if 'more_items' in parent_classes and 'core_load_request' in parent_classes: 
                        parent_data_tag_id = parent_div.get_attribute('data-tag_id')

                new_items += 1
                location_obj = {
                    'name': item_name,
                    'href': item_href,
                    'total_startups': item_total_startups,
                    'total_investors': item_total_investors,
                    'total_followers': item_total_followers,
                    'total_jobs': item_total_jobs,
                    'data_tag_id' : item_data_tag_id,
                    'parent_tag_id': parent_data_tag_id
                }

                uniq_locations[item_name] = location_obj
                locations_dict[item_name] = location_obj
                locations_by_id_dict[item_data_tag_id] = parent_data_tag_id

                print(item_name)
                print(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
                print('Item data id:', item_data_tag_id)
                print('Parent data id:', parent_data_tag_id)
                print(item_total_startups, item_total_investors, item_total_followers, item_total_jobs)
                print('Total locations crawled:', len(locations_dict))
                
                if item_total_startups >= 370:
                    try:
                        item.find_element_by_xpath('.//div[@class="clickable_area"]').click()
                        print('CLICK <clickable_area>')
                        time.sleep(0.5)
                    except:
                        print(item_name)
                        pass # item does not have "clickable_area"
                print('-'*40)

            print('\n')
            print('+'*40, '\n', 'New items:', new_items)
            print('+'*40, '\n')

        browser.quit()
        print('Total crawled locations:', len(locations_dict))
        pickle.dump(locations_dict, open(PICKLE_LOCATIONS, 'wb' ), protocol=pickle.HIGHEST_PROTOCOL)
    
    print('='*70)
    print('Finish: Crawl and store all locations from AngelList')

    return locations_dict
###############################################################################
def download_all_markets(): 
    print('\nStart: Crawl and store all markets from AngelList')
    print('='*70)
    sys.stdout.flush()

    markets_dict = dict()
    markets_by_id_dict = dict()
    uniq_markets = dict()

    # If file exist and size bigger than 1MB then return
    if os.path.exists(PICKLE_MARKETS) and os.path.getsize(PICKLE_MARKETS)/(1024) > 1:
        print('    ALREADY EXIST:', os.path.exists(PICKLE_MARKETS))
        print('   ', round(os.path.getsize(PICKLE_MARKETS)/(1024),2), 'KB')

        # Load data (deserialize)
        pickle_handle = open(PICKLE_MARKETS, 'rb')
        markets_dict = pickle.load(pickle_handle)
    else:
        print('\nStart Crawling...')

        # Get selenium instance
        browser = get_browser()

        # Get initial website of AngelList
        browser.get(ANGELLIST_MARKETS_URL)
        time.sleep(3)

        new_items = -1

        while new_items != 0:

            new_items = 0
            time.sleep(2)

            for item in browser.find_elements_by_class_name('items'):

                if item.text == 'More':
                    item.click()
                    print('CLICK <more>')
                    print(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
                    print('-'*40)
                    # time.sleep(1)
                    continue

                html_source = item.get_attribute('outerHTML')
                sourcedata = html_source.encode('utf-8')
                soup = BeautifulSoup(sourcedata, 'html.parser')

                try:
                    # item_name = item.find_element_by_xpath('.//div[@class="item-tag"]/a').get_attribute('textContent')
                    # item_href = item.find_element_by_xpath('.//div[@class="item-tag"]/a').get_attribute('href')
                    # item_stats_startups = item.find_element_by_xpath('.//div[@class="tag-stats"]/div[@class="item-startups"]')
                    item_tag = soup.find('div', {'class': 'item-tag'})
                    item_name = item_tag.text.replace('\n', '')
                    item_href = item_tag.find('a')['href']
                    item_stats_startups = soup.find('div', {'class': 'item-startups'})

                    res1 = soup.find('div', {'class': 'collapsed'})
                    res2 = soup.find('div', {'class': 'loaded'})

                    if item_name == 'Market' or item_name == 'All Markets' or item_name in uniq_markets or res1 != None or res2 != None:
                        continue
                except:
                    continue
                
                item_total_jobs = int(soup.find('div', {'class': 'item-jobs'}).find('a').text.replace(',', ''))
                item_total_followers = int(soup.find('div', {'class': 'item-followers'}).find('a').text.replace(',', ''))
                item_total_investors = int(soup.find('div', {'class': 'item-investors'}).find('a').text.replace(',', ''))
                item_total_startups = int(soup.find('div', {'class': 'item-startups'}).find('a').text.replace(',', ''))
                item_data_tag_id = soup.find('div', {'class': 'items'})['data-tag_id']

                parent_data_tag_id = '' # change it to zero for zero node
                parent_div = item.find_element_by_xpath('../../..')
                parent_classes = parent_div.get_attribute('class')

                if 'more_items' in parent_classes and 'core_load_request' in parent_classes: 
                    parent_data_tag_id = parent_div.get_attribute('data-tag_id')
                else:
                    parent_div = item.find_element_by_xpath('../../../..')
                    parent_classes = parent_div.get_attribute('class')
                    if 'more_items' in parent_classes and 'core_load_request' in parent_classes: 
                        parent_data_tag_id = parent_div.get_attribute('data-tag_id')       

                
                new_items += 1
                obj = {
                    'name': item_name,
                    'href': item_href,
                    'total_startups': item_total_startups,
                    'total_investors': item_total_investors,
                    'total_followers': item_total_followers,
                    'total_jobs': item_total_jobs,
                    'data_tag_id' : item_data_tag_id,
                    'parent_tag_id': parent_data_tag_id
                }

                uniq_markets[item_name] = obj
                markets_dict[item_name] = obj
                markets_by_id_dict[item_data_tag_id] = parent_data_tag_id

                print(item_name)
                print(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
                print('Item data id:', item_data_tag_id)
                print('Parent data id:', parent_data_tag_id)
                print(item_total_startups, item_total_investors, item_total_followers, item_total_jobs)
                print('Total markets crawled:', len(markets_dict))
                print('******NEW******')

                if item_total_startups >= 370:
                    try:
                        item.find_element_by_xpath('.//div[@class="clickable_area"]').click()
                        print('CLICK <clickable_area>')
                        time.sleep(0.5)
                    except:
                        pass # item does not have "clickable_area"

                print('-'*40)

            print('\n')
            print('+'*40, '\n', 'New items:', new_items)
            print('+'*40, '\n')

        browser.quit()
        print('Total crawled markets:', len(markets_dict))
        pickle.dump(markets_dict, open(PICKLE_MARKETS, 'wb' ), protocol=pickle.HIGHEST_PROTOCOL)
    
    print('='*70)
    print('Finish: Crawl and store all markets from AngelList\n')

    return markets_dict
###############################################################################
def create_total_raised_filter():
    print('Start: Create total raised filters...')
    raised_thresholds = [0, 1000]

    n = 1000
    while n < 100000000000:
        for i in range(2, 10):
            raised_thresholds.append(n*i)

        n = n*10
        raised_thresholds.append(n)

    raised_ranges = {}
    for i in range(1, len(raised_thresholds)):
        key = str(raised_thresholds[i-1]) + '_' + str(raised_thresholds[i])
        raised_ranges[key] = dict()
        raised_ranges[key]['url_param'] = '&raised[min]=%s&raised[max]=%s' % (str(raised_thresholds[i-1]),str(raised_thresholds[i]))

    # print(raised_ranges)
    print('Finish: Create total raised filters...')
    return raised_ranges
###############################################################################
def create_signal_filter():
    print('Start: Create signal filters...')
    signal_thresholds = []

    n = 0.0
    while n <= 10:
        signal_thresholds.append(round(n,1))
        n += 0.1

    signals_ranges = {}
    for i in range(1, len(signal_thresholds), 1):
        key = str(signal_thresholds[i-1]) + '_' + str(signal_thresholds[i])
        signals_ranges[key] = dict()
        signals_ranges[key]['url_param'] = '&signal[min]=%s&signal[max]=%s' % (str(signal_thresholds[i-1]),str(signal_thresholds[i]))

    # print(signals_ranges)
    print('Finish: Create signal filters...')
    return signals_ranges
###############################################################################
def create_layers_locations_markets(crawled_dict, dict_type):

    # crawled_dict= {
        # 'name': item_name, 'href': item_href,
        # 'total_startups': item_total_startups, 'total_investors': item_total_investors,
        # 'total_followers': item_total_followers, 'total_jobs': item_total_jobs,
        # 'data_tag_id' : item_data_tag_id, 'parent_tag_id': parent_data_tag_id
    # }

    data_id_dict = dict()

    # Find first layer of countries/markets e.g. Europe, Asia, etc.
    for key in crawled_dict:
        if crawled_dict[key]['parent_tag_id'] == '':
            data_tag_id = crawled_dict[key]['data_tag_id']

            crawled_dict[key]['layer'] = 'layer_1'
            crawled_dict[key]['url_location'] = data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')

            if dict_type == 'locations':
                crawled_dict[key]['url_param'] = '&locations[]=' + data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')
            elif dict_type == 'markets':
                crawled_dict[key]['url_param'] = '&markets[]=' + crawled_dict[key]['name'].replace(' ', '+')

            data_id_dict[data_tag_id] = {
                'layer': 'layer_1',
                'name': crawled_dict[key]['name'],
                'url_location': data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')
            }


    # Connect the rest of the countries/markets
    i = 2
    while len(crawled_dict) != len(data_id_dict):
        for key in crawled_dict:
            data_tag_id = crawled_dict[key]['data_tag_id']
            parent_tag_id = crawled_dict[key]['parent_tag_id']

            if not 'layer' in crawled_dict[key] and parent_tag_id in data_id_dict:

                crawled_dict[key]['layer'] = 'layer_' + str(i)
                crawled_dict[key]['url_location'] = data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')
                crawled_dict[key]['parent_name'] = data_id_dict[parent_tag_id]['name']

                if dict_type == 'locations':
                    crawled_dict[key]['url_param'] = '&locations[]=' + data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')
                elif dict_type == 'markets':
                    crawled_dict[key]['url_param'] = '&markets[]=' + crawled_dict[key]['name'].replace(' ', '+')

                data_id_dict[data_tag_id] = {
                    'layer': 'layer_' + str(i),
                    'name': crawled_dict[key]['name'],
                    'url_location': data_tag_id + '-' + crawled_dict[key]['name'].replace(' ', '+')
                }

        i += 1

    return crawled_dict
###############################################################################
def get_specific_layer(crawled_dict, layer_name):
    # Get specific layer from the dict
    res_dict = dict()
    for key in crawled_dict:
        if crawled_dict[key]['layer'] == layer_name:
            res_dict[key] = crawled_dict[key]

    return res_dict
###############################################################################
def get_specific_subtree(crawled_dict, item_name):
    # Get specific subtree from the dict
    res_dict = dict()
    for key in crawled_dict:
        if crawled_dict[key]['parent_name'] == item_name:
            res_dict[key] = crawled_dict[key]

    return res_dict
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
            print('        ', 'Total companies: ' + str(total_companies))
        except:
            print(traceback.format_exc())
            print('        ', 'Class <count> does not exist')
            num_errors += 1
            time.sleep(num_errors*3)

    if total_companies == -1:
        print('='*50)
        print("EXIT...3 errors")
        exit()

    return total_companies
###############################################################################
def clean_queue(consumer):
    print('\nClean queue...')
    # consume and clean messages from topic 'results'
    for msg in consumer:
        consumer.commit()
    print('Done\n')
###############################################################################

###############################################################################
def master_first_stage_start(raised_ranges, signal_ranges, locations_dict, markets_dict):
    print('Start: master_first_stage_start')
    print('='*50)

    # FILTERS
    # a. startups: https://angel.co/companies?company_types[]=Startup
    # b. total raised: &raised[min]=0&raised[max]=0
    # c. signals_thresholds: &signal[min]=0&signal[max]=1.1
    # d. countries: &locations[]=1688-United+States
    # e. markets: &markets[]=E-Commerce

    # Initialize Kafka producer and consumer
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, 
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, 
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="earliest", consumer_timeout_ms=1000, 
        enable_auto_commit=True, group_id='res-1')
    consumer.subscribe(KAFKA_TOPIC_RESULTS)
    clean_queue(consumer)

    # Initialize browser
    # browser = get_browser()
    # Get cookies for AngelList account
    # get_cookies(browser, COOKIE_FILE)

    # Connect to MongoDB
    client = MongoClient(DATABASE_IP, DATABASE_PORT)
    col_orgs = client[DATABASE_NAME][DATABASE_ORGS_COLLECTION]

    client2 = MongoClient(DATABASE_IP, DATABASE_PORT)
    col_queries = client2[DATABASE_NAME][DATABASE_QUERIES_COLLECTION]
    # Find all successful queries
    for doc in col_queries.find({}):
        success_queries[doc['_id']] = doc['total_companies']

    # Get total companies for this search
    # print('Current total companies...')
    # total_companies = get_orgs_number(browser, ANGELLIST_STARTUPS_URL)
    print('Static total companies...')
    total_companies = 38530

    crawlable_companies = 0
    not_crawlable_companies = 0

    locations_layer_1 = get_specific_layer(locations_dict, 'layer_1')
    locations_layer_2 = get_specific_layer(locations_dict, 'layer_2')
    market_layer_1 = get_specific_layer(markets_dict, 'layer_1')
    market_layer_2 = get_specific_layer(markets_dict, 'layer_2')
    print('Layer 1 locations: ' + str(len(locations_layer_1)))
    print('Layer 2 locations: ' + str(len(locations_layer_2)))
    print('Layer 1 market: ' + str(len(market_layer_1)))
    print('Layer 2 market: ' + str(len(market_layer_2)))

    #############################################
    layer_results = {'': { 'url': ANGELLIST_STARTUPS_URL, 'total_companies': total_companies, 'all_keys': []}}

    ### Layer 1 ###
    queries_dict = create_queries(raised_ranges, layer_results)
    layer_results, not_crawlable_companies = queries_workflow(producer, consumer, col_orgs, col_queries, queries_dict)
    print('='*50)

    ### Layer 2 ###
    queries_dict = create_queries(signal_ranges, layer_results)
    layer_results, not_crawlable_companies = queries_workflow(producer, consumer, col_orgs, col_queries, queries_dict)
    print('='*50)

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # for countries: 2nd layer:
    # remove countries from: Oceania, South America, Africa 
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    ### Layer 3 ###
    # queries_dict = create_queries(locations_layer_1, layer_results)
    # layer_results, not_crawlable_companies = queries_workflow(producer, consumer, col_orgs, col_queries, queries_dict)
    # print('='*50)
    queries_dict = create_queries(market_layer_1, layer_results)
    layer_results, not_crawlable_companies = queries_workflow(producer, consumer, col_orgs, col_queries, queries_dict)
    print('='*50)
    #############################################

    print('\nTotal companies: ' + str(total_companies))
    print('Companies in Mongo: ' + str(col_orgs.find().count()))
    # print('Crawlable companies: ' + str(crawlable_companies))
    # print('NOT Crawlable companies: ' + str(not_crawlable_companies))
    
    print('='*50)
    print('Finish: master_first_stage_start')
###############################################################################
def create_queries(filters_dict, previous_layer):
    """  Create queries based on the filters and the previous results """

    # Create queries
    queries_dict = dict()
    for key_i in previous_layer:

        if previous_layer[key_i]['total_companies'] > 400 or previous_layer[key_i]['total_companies'] == -1:

            for key_j in filters_dict:
                new_key = key_i + '_' + key_j
                queries_dict[new_key] = dict()
                queries_dict[new_key]['url'] = previous_layer[key_i]['url'] + filters_dict[key_j]['url_param']
                
                queries_dict[new_key]['previous_url'] = previous_layer[key_i]['url']
                queries_dict[new_key]['total_companies'] = -1
                queries_dict[new_key]['all_keys'] = previous_layer[key_i]['all_keys'].copy()
                queries_dict[new_key]['all_keys'].append(key_j)

    return queries_dict
###############################################################################
def queries_workflow(producer, consumer, col_orgs, col_queries, queries_dict):
    # Producer
    produce_queries(producer, queries_dict)

    # Consumer
    results = consume_results(consumer, queries_dict, col_orgs, col_queries)

    # Process results
    queries_dict, not_crawlable_companies = process_results(results, queries_dict)

    return queries_dict, not_crawlable_companies
###############################################################################
def produce_queries(producer, queries_dict):
    print('\n' + str(CUR_LAYER) + ' layer - Total queries: ' + str(len(queries_dict)))
    pending_queries = 0
    i = 0
    for key in queries_dict:
        params_key = queries_dict[key]['url'].replace(ANGELLIST_STARTUPS_URL, '')

        if not params_key in success_queries:

            data = {
                'item_i' : i,  
                'url': queries_dict[key]['url'], 
                'key': key,
                'params_key': params_key,
                'layer': CUR_LAYER, 'stage': '1',
                'datetime': datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            }
            producer.send(KAFKA_TOPIC_QUERIES, value=data)
            pending_queries += 1
        else: 
            # Already DONE, stage=completed
            data = {
                'item_i' : i,  
                'url': queries_dict[key]['url'], 
                'key': key,
                'params_key': params_key,
                'layer': CUR_LAYER, 'stage': 'completed',
                'datetime': datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                'total_companies': success_queries[params_key],
                'crawled_orgs': {}
            }
            producer.send(KAFKA_TOPIC_RESULTS, value=data)
            # producer.send(KAFKA_TOPIC_QUERIES, value=data)
        i += 1

    print('     Total left queries: ' + str(pending_queries))

    print('-'*40)
###############################################################################
def consume_results(consumer, queries_dict, col_orgs, col_queries):
    print(str(CUR_LAYER) + ' layer - Consume results...')
    print('-'*40)

    results = {}
    count_msg = 0
    while count_msg < len(queries_dict):
        for msg in consumer:
            # print('    ', msg.value)
            count_msg += 1

            results[msg.value['key']] = msg.value
            res = msg.value

            # Store crawled organizations
            for org_id in res['crawled_orgs']:
                # If company already DOES NOT EXIST then Store to MongoDB
                if col_orgs.find({'name': res['crawled_orgs'][org_id]['name']}).count() == 0:
                    col_orgs.insert(res['crawled_orgs'][org_id])

            # Store query
            if res['stage'] != 'completed':
                # Store successful query to database
                col_queries.insert({'_id': res['params_key'], 'total_companies': res['total_companies']})

                print('    ' + 'Consume message: ' + str(res['item_i']) + ', ' 
                    'Total messages consumed:' + str(count_msg) + ', '
                    + str(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')) )

            consumer.commit()

    print('-'*40)
    return results
###############################################################################
def process_results(results, queries_dict):
    global CUR_LAYER

    print(str(CUR_LAYER) + ' layer - Print if bigger than 400...')
    print('-'*40)

    uncompleted_queries = 0
    crawlable_companies = 0
    not_crawlable_companies = 0
    for key in results:
        queries_dict[key]['total_companies'] = results[key]['total_companies']

        if results[key]['total_companies'] > 400:
            print('    ', results[key])
            not_crawlable_companies += results[key]['total_companies'] - 400
            uncompleted_queries += 1

    print('Queries for next layer:', uncompleted_queries)
    print('-'*40)
    CUR_LAYER += 1

    return queries_dict, not_crawlable_companies
###############################################################################


###############################################################################
def first_layer(raised_ranges):
    """  First layer with raised ranges """

    # Create queries
    queries_dict = dict()
    for raised_list in raised_ranges:
        key = str(raised_list[0]) + '_' + str(raised_list[1])
        queries_dict[key] = dict()
        queries_dict[key]['url'] = ANGELLIST_STARTUPS_URL + '&raised[min]=%s&raised[max]=%s' % (str(raised_list[0]),str(raised_list[1]))
        queries_dict[key]['total_companies'] = -1
        queries_dict[key]['all_keys'] = [key]

    return queries_dict
###############################################################################
def first_layer_v2(raised_ranges, previous_layer):
    """  First layer with raised ranges """

    # Create queries
    queries_dict = dict()
    for key_i in previous_layer:

        if previous_layer[key_i]['total_companies'] > 400:

            for raised_list in raised_ranges:
                key = str(raised_list[0]) + '_' + str(raised_list[1])
                queries_dict[key] = dict()
                queries_dict[key]['url'] = previous_layer[key_i]['url'] + '&raised[min]=%s&raised[max]=%s' % (str(raised_list[0]),str(raised_list[1]))
                queries_dict[key]['total_companies'] = -1
                queries_dict[key]['all_keys'] = previous_layer[key_i]['all_keys'].append(key)

    return queries_dict
###############################################################################
def second_layer(signal_ranges, previous_layer):
    """ Second layer with signal ranges """

    # Create queries
    queries_dict = dict()
    for key_i in previous_layer:

        if previous_layer[key_i]['total_companies'] > 400:

            for signals_list in signal_ranges:
                key_j = key_i + '_' + str(signals_list[0]) + '_' + str(signals_list[1])
                queries_dict[key_j] = dict()
                queries_dict[key_j]['url'] = previous_layer[key_i]['url'] + '&signal[min]=%s&signal[max]=%s' % (str(signals_list[0]),str(signals_list[1]))
                queries_dict[key_j]['total_companies'] = -1
                # queries_dict[key_j]['all_keys'] = previous_layer[key_i]['all_keys']
                # queries_dict[key_j]['all_keys'].append(key_j)

    return queries_dict
###############################################################################
def third_layer(locations_layer, previous_layer):
    """ Third layer with locations-1st-layer """

    # Create queries
    queries_dict = dict()
    for key_i in previous_layer:

        if previous_layer[key_i]['total_companies'] > 400:

            for key_j in locations_layer:
                new_key_j = key_i + '_' + key_j
                queries_dict[new_key_j] = dict()
                queries_dict[new_key_j]['url'] = previous_layer[key_i]['url'] + '&locations[]=%s' % (locations_layer[key_j]['url_location'])
                queries_dict[new_key_j]['previous_url'] = previous_layer[key_i]['url']
                queries_dict[new_key_j]['location_key'] = key_j
                queries_dict[new_key_j]['total_companies'] = -1
                # queries_dict[new_key_j]['all_keys'] = previous_layer[key_i]['all_keys']
                # queries_dict[new_key_j]['all_keys'].append(key_j)

    return queries_dict
###############################################################################


if __name__ == "__main__":

    # Prepare filters for queries
    locations_dict = download_all_locations()
    locations_dict_v2 = create_layers_locations_markets(locations_dict, 'locations')
    markets_dict = download_all_markets()
    markets_dict_v2 = create_layers_locations_markets(markets_dict, 'markets')
    raised_ranges = create_total_raised_filter()
    signal_ranges = create_signal_filter()

    # Start master node for crawling
    master_first_stage_start(raised_ranges, signal_ranges, locations_dict_v2, markets_dict_v2)


