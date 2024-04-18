#encoding=utf8

###############################################################################
### Libraries ###
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
from selenium.webdriver.firefox.options import Options
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import os, sys, datetime, re, time, json, traceback, random, logging, pickle, requests
from pymongo import MongoClient
import config_angellist as config
from pyvirtualdisplay import Display
from queue import Queue
from threading import Thread
import threading
from concurrent import futures
###############################################################################

###############################################################################
### Global Variables ###

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
DATABASE_PORT = 27017
# Database name for storing AngelList
DATABASE_NAME = 'AngelList_12_2019'
# Collection name for storing AngelList organizations
DATABASE_ORGS_COLLECTION = 'organizations'
# Collection name for storing AngelList organizations
# DATABASE_PEOPLE_COLLECTION = 'people'
# DATABASE_FUNDING_ROUNDS_COLLECTION = 'funding_rounds'
# DATABASE_RELAT_PEOPLE_COLLECTION = 'Relationships_people'
# DATABASE_RELAT_ORGS_COLLECTION = 'Relationships_organizations'

# Data filenames for pickles
PICKLE_LOCATIONS = 'pickles/locations_dict.pickle'
PICKLE_MARKETS = 'pickles/markets_dict.pickle'

# Cookies dir
COOKIE_FILE = "cookies/cookie-N.pkl"

# Multithreading variables
queue = Queue()
NUM_THREADS=1
browsers = []
db_connections = []

# Start display (for VM)
# display = Display(visible=0, size=(800, 800))
# display.start()

###############################################################################
def get_browser():
    # Selenium options for headless browser
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    # The browser (firefox or chrome) that Selenium will use
    # browser = webdriver.Chrome(chrome_options=options, executable_path='/usr/local/bin/chromedriver')
    browser = webdriver.Firefox(firefox_options=options, executable_path='/usr/local/bin/geckodriver')

    browsers.append(browser)

    return browser
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
    print('Finish: Crawl and store all markets from AngelList')

    return markets_dict
###############################################################################
def angellist_parallel_crawler(locations_dict, markets_dict):

    print('\nStart: Parallel crawler')
    print('='*70)

    # Get selenium instance
    browser = get_browser()

    # Get initial website of AngelList
    browser.get(ANGELLIST_STARTUPS_URL)
    time.sleep(5)

    # Create filters
    # raised_ranges = create_total_raised_filter()
    raised_ranges = [[0, 1000]]
    # signals_ranges = create_signal_filter()
    signals_ranges = [
        [0.0, 0.1], [0.2, 0.3], [0.4, 0.5], 
        [0.6, 0.7], [0.8, 0.9], [1.0, 1.1], 
        [3.9, 4.0], [4.9, 5.0], [5.9, 6.0], 
        [6.9, 7.0], [7.9, 8.0], [8.9, 9.0], 
        [9.9, 10.0]
    ]

    locations_dict_v2 = create_layers(locations_dict)
    markets_dict_v2 = create_layers(markets_dict)

    locations_layer_1 = get_specific_layer(locations_dict_v2, 'layer_1')

    # Combine filters
    filters_dict = dict()
    basic_url = 'https://angel.co/companies?company_types[]=Startup'
    for raised_list in raised_ranges:
        key_1 = str(raised_list[0]) + '_' + str(raised_list[1])
        filters_dict[key_1] = dict()
        filters_dict[key_1]['url'] = basic_url + '&raised[min]=%s&raised[max]=%s' % (str(raised_list[0]),str(raised_list[1]))
        filters_dict[key_1]['total_companies'] = -1
        filters_dict[key_1]['next'] = dict()

        for signals_list in signals_ranges:
            key_2 = str(signals_list[0]) + '_' + str(signals_list[1])
            filters_dict[key_1]['next'][key_2] = dict()
            filters_dict[key_1]['next'][key_2]['url'] = filters_dict[key_1]['url'] + '&signal[min]=%s&signal[max]=%s' % (str(signals_list[0]),str(signals_list[1]))
            filters_dict[key_1]['next'][key_2]['total_companies'] = -1
            filters_dict[key_1]['next'][key_2]['next'] = dict()

            for loc_lay1 in locations_layer_1:
                url_location = locations_layer_1[loc_lay1]['url_location']

                filters_dict[key_1]['next'][key_2]['next'][loc_lay1] = dict()
                filters_dict[key_1]['next'][key_2]['next'][loc_lay1]['url'] = filters_dict[key_1]['next'][key_2]['url'] + '&locations[]=%s' % (url_location)
                filters_dict[key_1]['next'][key_2]['next'][loc_lay1]['total_companies'] = -1
                filters_dict[key_1]['next'][key_2]['next'][loc_lay1]['next'] = dict()


    # Get total companies for this search
    total_companies = get_orgs_number(-1, browser)
    print('Main', ', Total companies: ' + str(total_companies))

    # Initialize pool of threads
    pool = futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
    # Initialize threads
    print('Initialize threads...')
    for thread_id in range(0, NUM_THREADS):
        # Get selenium instance
        new_browser = get_browser()

        # Get cookies for AngelList account
        get_cookies(new_browser, COOKIE_FILE.replace('N', str(thread_id)))

        # Connect to MongoDB
        client = MongoClient(DATABASE_IP, DATABASE_PORT)
        collection_orgs = client[DATABASE_NAME][DATABASE_ORGS_COLLECTION]
        db_connections.append(collection_orgs)


    crawlable_companies = 0
    not_crawlable_companies = 0
    ######################################################
    ### 1st Layer ###
    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_1) to threads...\n')
    for key_1 in filters_dict:
        task = pool.submit(thread_selenium, i, filters_dict[key_1]['url'], [key_1])
        res.append(task)
        i += 1
    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('='*50)
    for i in range(0, len(res)):
        res_tmp = res[i].result()
        filters_dict[res_tmp['key'][0]]['total_companies'] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print(res_tmp)
    ######################################################
    ### 2nd Layer ###
    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_2) to threads...\n')
    for key_1 in filters_dict:
        if filters_dict[key_1]['total_companies'] > 400:
            
            for key_2 in filters_dict[key_1]['next']:
                task = pool.submit(thread_selenium, i, filters_dict[key_1]['next'][key_2]['url'], [key_1, key_2])
                res.append(task)
                i += 1

    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('='*50)
    for i in range(0, len(res)):
        res_tmp = res[i].result()

        filters_dict[res_tmp['key'][0]]['next'][res_tmp['key'][1]]['total_companies'] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print(res_tmp)
    ######################################################
    ### 3rd Layer ###
    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_3) to threads...\n')
    for key_1 in filters_dict:

        if filters_dict[key_1]['total_companies'] > 400:
        
            for key_2 in filters_dict[key_1]['next']:

                if filters_dict[key_1]['next'][key_2]['total_companies'] > 400:

                    for key_3 in filters_dict[key_1]['next'][key_2]['next']:

                        task = pool.submit(thread_selenium, i, filters_dict[key_1]['next'][key_2]['next'][key_3]['url'], [key_1, key_2, key_3])
                        res.append(task)
                        i += 1

    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('='*50)
    for i in range(0, len(res)):
        res_tmp = res[i].result()

        filters_dict[ res_tmp['key'][0] ][ 'next' ][ res_tmp['key'][1] ][ 'next' ][ res_tmp['key'][2] ][ 'total_companies' ] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print(res_tmp)
            not_crawlable_companies += res_tmp['total_companies'] - 400
        else:
            crawlable_companies += res_tmp['total_companies']

    ######################################################
    print('\nMain', ', Total companies: ' + str(total_companies))
    print('Main', ', Crawlable companies: ' + str(crawlable_companies))
    print('Main', ', NOT Crawlable companies: ' + str(not_crawlable_companies))


    # FILTERS
    # a. startups
    # b. total raised
    # c. signals_thresholds
    # d. countries or markets (depending on the total_markets vs total_countries)
    # e. markets or countries (depending on the total_markets vs total_countries)
    
    # https://angel.co/companies?company_types[]=Startup
    # &raised[min]=0&raised[max]=0
    # &signal[min]=0&signal[max]=1.1
    # &locations[]=1688-United+States&markets[]=E-Commerce

    # Close all instances of selenium
    print('\nClose all browsers instances...')
    for driver in browsers:
        driver.quit()

    # quit Xvfb display
    # display.stop()

    print('='*70)
    print('Finish: Parallel crawler')
###############################################################################
def angellist_parallel_crawler_3(locations_dict, markets_dict):
    # FILTERS
    # a. startups
    #    https://angel.co/companies?company_types[]=Startup
    # b. total raised
    #    &raised[min]=0&raised[max]=0
    # c. signals_thresholds
    #    &signal[min]=0&signal[max]=1.1
    # d. countries or markets (depending on the total_markets vs total_countries)
    #    &locations[]=1688-United+States
    # e. markets or countries (depending on the total_markets vs total_countries)
    #    &markets[]=E-Commerce

    print('\nStart: Parallel crawler')
    print('='*70)

    # Get selenium instance
    browser = get_browser()

    # Get initial website of AngelList
    browser.get(ANGELLIST_STARTUPS_URL)
    time.sleep(5)

    # Get total companies for this search
    total_companies = get_orgs_number(-1, browser)
    print('Main', ', Total companies: ' + str(total_companies))

    # Initialize pool of threads
    pool = futures.ThreadPoolExecutor(max_workers=NUM_THREADS)
    # Initialize threads
    print('Initialize threads...')
    for thread_id in range(0, NUM_THREADS):
        # Get selenium instance
        new_browser = get_browser()

        # Get cookies for AngelList account
        get_cookies(new_browser, COOKIE_FILE.replace('N', str(thread_id)))

        # Connect to MongoDB
        client = MongoClient(DATABASE_IP, DATABASE_PORT)
        collection_orgs = client[DATABASE_NAME][DATABASE_ORGS_COLLECTION]
        db_connections.append(collection_orgs)

    crawlable_companies = 0
    not_crawlable_companies = 0

    # Create filters
    # raised_ranges = create_total_raised_filter()
    raised_ranges = [[0, 1000]]
    # signals_ranges = create_signal_filter()
    signals_ranges = [
        [0.0, 0.1], [0.2, 0.3], [0.4, 0.5], 
        [0.6, 0.7], [0.8, 0.9], [1.0, 1.1], 
        [3.9, 4.0], [4.9, 5.0], [5.9, 6.0], 
        [6.9, 7.0], [7.9, 8.0], [8.9, 9.0], 
        [9.9, 10.0]
    ]

    locations_dict_v2 = create_layers(locations_dict)
    markets_dict_v2 = create_layers(markets_dict)

    locations_layer_1 = get_specific_layer(locations_dict_v2, 'layer_1')


    #############################################################
    ### 1st Layer ###
    filters_layer1_dict = dict()
    basic_url = 'https://angel.co/companies?company_types[]=Startup'
    for raised_list in raised_ranges:
        key = str(raised_list[0]) + '_' + str(raised_list[1])
        filters_layer1_dict[key] = dict()
        filters_layer1_dict[key]['url'] = basic_url + '&raised[min]=%s&raised[max]=%s' % (str(raised_list[0]),str(raised_list[1]))
        filters_layer1_dict[key]['total_companies'] = -1
        filters_layer1_dict[key]['all_keys'] = [key]

    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_1) to threads...\n')
    for key in filters_layer1_dict:
        task = pool.submit(thread_selenium, i, filters_layer1_dict[key]['url'], key, filters_layer1_dict[key]['all_keys'])
        res.append(task)
        i += 1
    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('-'*40)
    for i in range(0, len(res)):
        res_tmp = res[i].result()
        filters_layer1_dict[res_tmp['key']]['total_companies'] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print(res_tmp)

    #############################################################
    ### 2nd Layer ###
    filters_layer2_dict = dict()

    for key_i in filters_layer1_dict:

        if filters_layer1_dict[key_i]['total_companies'] > 400:

            for signals_list in signals_ranges:
                key_j = key_i + '_' + str(signals_list[0]) + '_' + str(signals_list[1])
                filters_layer2_dict[key_j] = dict()
                filters_layer2_dict[key_j]['url'] = filters_layer1_dict[key_i]['url'] + '&signal[min]=%s&signal[max]=%s' % (str(signals_list[0]),str(signals_list[1]))
                filters_layer2_dict[key_j]['total_companies'] = -1

                filters_layer2_dict[key_j]['all_keys'] = filters_layer1_dict[key_i]['all_keys']
                filters_layer2_dict[key_j]['all_keys'].append(key_j)
                

    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_2) to threads...\n')
    for key in filters_layer2_dict:
        task = pool.submit(thread_selenium, i, filters_layer2_dict[key]['url'], key, filters_layer2_dict[key]['all_keys'])
        res.append(task)
        i += 1
    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('-'*40)
    for i in range(0, len(res)):
        res_tmp = res[i].result()
        filters_layer2_dict[res_tmp['key']]['total_companies'] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print(res_tmp)

    #############################################################
    ### 3rd Layer ###
    filters_layer3_dict = dict()

    for key_i in filters_layer2_dict:

        if filters_layer2_dict[key_i]['total_companies'] > 400:

            for key_j in locations_layer_1:
                new_key_j = key_i + '_' + key_j
                filters_layer3_dict[new_key_j] = dict()
                filters_layer3_dict[new_key_j]['url'] = filters_layer2_dict[key_i]['url'] + '&locations[]=%s' % (locations_layer_1[key_j]['url_location'])
                filters_layer3_dict[new_key_j]['previous_url'] = filters_layer2_dict[key_i]['url']
                filters_layer3_dict[new_key_j]['total_companies'] = -1

                filters_layer3_dict[new_key_j]['all_keys'] = filters_layer2_dict[key_i]['all_keys']
                filters_layer3_dict[new_key_j]['all_keys'].append(key_j)

    res = []
    i = 0
    print('='*50)
    print('Put tasks (layer_3) to threads...\n')
    for key in filters_layer3_dict:
        task = pool.submit(thread_selenium, i, filters_layer3_dict[key]['url'], key, filters_layer3_dict[key]['all_keys'])
        res.append(task)
        i += 1
    finished, pending = futures.wait(res, return_when=futures.ALL_COMPLETED)

    print('-'*40)
    for i in range(0, len(res)):
        res_tmp = res[i].result()
        filters_layer3_dict[res_tmp['key']]['total_companies'] = res_tmp['total_companies']
        if res_tmp['total_companies'] > 400:
            print('    ', res_tmp['url'])
            del res_tmp['url']
            print(res_tmp)

            not_crawlable_companies += res_tmp['total_companies'] - 400

    ######################################################

    ######################################################
    print('\nMain', ', Total companies: ' + str(total_companies))
    # print('Main', ', Crawlable companies: ' + str(crawlable_companies))
    print('Main', ', NOT Crawlable companies: ' + str(not_crawlable_companies))

    # Close all instances of selenium
    print('\nClose all browsers instances...')
    for driver in browsers:
        driver.quit()

    # quit Xvfb display
    # display.stop()

    print('='*70)
    print('Finish: Parallel crawler')
###############################################################################
def create_layers(crawled_dict):
    # 'name': item_name,
    # 'href': item_href,
    # 'total_startups': item_total_startups,
    # 'total_investors': item_total_investors,
    # 'total_followers': item_total_followers,
    # 'total_jobs': item_total_jobs,
    # 'data_tag_id' : item_data_tag_id,
    # 'parent_tag_id': parent_data_tag_id

    data_id_dict = dict()

    # 1st Layer
    for key in crawled_dict:
        if crawled_dict[key]['parent_tag_id'] == '':

            crawled_dict[key]['layer'] = 'layer_1'
            crawled_dict[key]['url_location'] = crawled_dict[key]['data_tag_id'] + '-' + crawled_dict[key]['name'].replace(' ', '+')

            data_id_dict[crawled_dict[key]['data_tag_id']] = {
                'layer': 'layer_1',
                'name': crawled_dict[key]['name'],
                'url_location': crawled_dict[key]['data_tag_id'] + '-' + crawled_dict[key]['name'].replace(' ', '+')
            }

    i = 2
    while len(crawled_dict) != len(data_id_dict):
        for key in crawled_dict:
            if not 'layer' in crawled_dict[key] and crawled_dict[key]['parent_tag_id'] in data_id_dict:

                crawled_dict[key]['layer'] = 'layer_' + str(i)
                crawled_dict[key]['url_location'] = crawled_dict[key]['data_tag_id'] + '-' + crawled_dict[key]['name'].replace(' ', '+')
                crawled_dict[key]['parent_name'] = data_id_dict[crawled_dict[key]['parent_tag_id']]['name']

                data_id_dict[crawled_dict[key]['data_tag_id']] = {
                    'layer': 'layer_' + str(i),
                    'name': crawled_dict[key]['name'],
                    'url_location': crawled_dict[key]['data_tag_id'] + '-' + crawled_dict[key]['name'].replace(' ', '+')
                }

        i += 1
    # count_layers = dict()
    # for key in crawled_dict:
    #     # if 'layer' in crawled_dict[key] and crawled_dict[key]['layer'] == 'layer_3':
    #     #     print(crawled_dict[key]['name'])
        
    #     if not crawled_dict[key]['layer'] in count_layers:
    #         count_layers[crawled_dict[key]['layer']] = 0
    #     count_layers[crawled_dict[key]['layer']] += 1
    # print(count_layers)

    return crawled_dict
###############################################################################
def get_specific_layer(crawled_dict, layer_name):

    res_dict = dict()
    for key in crawled_dict:
        if crawled_dict[key]['layer'] == layer_name:
            res_dict[key] = crawled_dict[key]

    return res_dict
###############################################################################
def get_specific_subtree(crawled_dict, item_name):

    res_dict = dict()
    for key in crawled_dict:
        if crawled_dict[key]['parent_name'] == item_name:
            res_dict[key] = crawled_dict[key]

    return res_dict
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

    raised_ranges = []
    for i in range(1, len(raised_thresholds)):
        raised_ranges.append([raised_thresholds[i-1], raised_thresholds[i]])

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

    signals_ranges = []
    for i in range(1, len(signal_thresholds), 2):
        signals_ranges.append([signal_thresholds[i-1], signal_thresholds[i]])

    # print(signals_ranges)
    print('Finish: Create signal filters...')

    return signals_ranges
###############################################################################
def thread_selenium(num_item, url, key, all_keys):

    thread_id = int(str(threading.current_thread().getName()).replace('Thread-', '')) - 1
    print('i:' + str(num_item), ', Thread_id: ' + str(thread_id), ', Time: ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))

    # Connect to MongoDB
    collection_orgs = db_connections[thread_id]

    # Get selenium instance
    browser = browsers[thread_id]

    # Get initial website of AngelList
    browser.get(url)
    time.sleep(3)    

    # Get total companies for this search
    total_companies = get_orgs_number(thread_id, browser)
    # print('Thread_id: ' +  str(thread_id), ', Ranges: ' + str(filter_range), ', Total companies: ' + str(total_companies))

    # if zero then return with success
    # if less than 400 then crawl them and store them
    #   at the end: return success and the total number of companies
    # else 
    #    return: return failed and the total number of companies
    result_dict = {
        'key': key,
        'all_keys': all_keys,
        'total_companies': total_companies,
        'url': url
    }

    return result_dict
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

def angelist_orgs_crawler():

    # Connect to MongoDB
    client = MongoClient(DATABASE_IP, DATABASE_PORT)
    collection_orgs = client[DATABASE_NAME][DATABASE_ORGS_COLLECTION]

    # Get initial website of AngelList
    browser.get(ANGELLIST_STARTUPS_URL)
    time.sleep(5)

    # Get total companies for this search
    total_companies = get_orgs_number()


    companies_crawled = 0
    orgs_dict = dict()


    # Find results in the initial page
    class_results = ''
    errors_count = 0
    while(True):
        try:
            class_results = browser.find_element_by_class_name('results')
            break
        except:
            if errors_count == 3:
                browser.close()
                exit()
            print('Class <results> does not exist')
            print('Sleep for 5 seconds...')
            time.sleep(3);
            errors_count += 1

    print('\nStart collecting AngelList Companies...')
    print('='*50)
    
    # Continue getting companies by pressing "more" button/item
    errors_count = 0
    while(companies_crawled < total_companies):

        count_new_companies, orgs_dict = parse_and_store_results(orgs_dict, collection_orgs)

        companies_crawled += count_new_companies
        print('\n', '-' * 50, '\n' + datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'), ', Count_new_companies:', 
            str(count_new_companies), ', Companies_crawled:', str(companies_crawled))

        # Move to next items
        try:
            browser.find_element_by_class_name('more').click()
            time.sleep(5)
        except:
            print('\nButton "more" does not exist...maybe it is finished!')
            break


    print('='*50)
    print('Finish collecting AngelList Companies')

    # Close browser
    browser.close()
###############################################################################
def get_orgs_number(thread_id, browser):
    # Get total companies for crawling

    num_errors = 0
    flag = False
    total_companies = -1

    while num_errors < 3 and flag == False:
        try:
            class_count = browser.find_element_by_xpath("//div[contains(@class, 'count')]") 
            total_companies = int(class_count.text.replace(',', '').replace(' Companies', '').replace(' Company', ''))
            flag = True
        except:
            print(traceback.format_exc())
            print('   Thread_id: ', thread_id, ', Class <count> does not exist')
            num_errors += 1
            time.sleep(num_errors)

    return total_companies
###############################################################################
def parse_and_store_results(orgs_dict, collection_orgs):

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
            'total_raised': '',
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
        company_fields['website'] = company_website[0].select('a')[0]['href']

        # Employees column
        company_size = company_row.select('div.column.company_size')
        company_fields['size'] = company_size[0].select('div.value')[0].text.strip()
        if len(company_fields['size']) == '-':
            company_fields['size'] = ''

        # Stage column
        company_stage = company_row.select('div.column.stage')
        company_fields['stage'] = company_stage[0].select('div.value')[0].text
        company_fields['stage'] = company_fields['stage'].strip().replace('-', '')

        # Total raised column
        company_raised = company_row.select('div.column.raised')
        company_fields['total_raised'] = company_raised[0].select('div.value')[0].text
        company_fields['total_raised'] = company_fields['total_raised'].strip().replace(',','').replace('-', '')

        if not company_fields['name'] in orgs_dict:
            orgs_dict[company_fields['name']] = 1
            count_new_companies += 1

            # If company already DOES NOT EXIST then Store to MongoDB
            if collection_orgs.find({'name': company_fields['name']}).count() == 0:
                collection_orgs.insert(company_fields)


    return count_new_companies, orgs_dict
###############################################################################

if __name__ == "__main__":

    # Download the countries from AngelList
    locations_dict = download_all_locations()
    markets_dict = download_all_markets()

    # Start parallel selenium crawler
    # angellist_parallel_crawler(locations_dict, markets_dict)
    angellist_parallel_crawler_3(locations_dict, markets_dict)
    
    # pkill chrome; pkill firefox; pkill Xvfb
