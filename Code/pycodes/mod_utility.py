#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import warnings
warnings.filterwarnings("ignore")

## Importing Selenium and Drivers

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.chrome.options import Options

from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import requests
import time
import json
import datetime

def get_driver_data(url:str,params_iter : dict):
    """
    
    Get the html data for the page opened by Selenium driver
    ------------------------------------
    
    Input:
    url (str) : url from which data is to be parsed 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    
    ------------------------------------
    Output:
    Selenium.webdriver.Chrome object : Contains data about the page that needs to be parsed 
    
    """    
    
    SCROLL_PAUSE_TIME = params_iter['scroll_wait_time']
    
    if params_iter['silent_mode']:
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=options)
    else:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    
    
    ## Check if the URL has correct format
    if not url_validator(url):
        raise ValueError("The URL "&url&" is not a valid URL format")
        pass
    
    driver.get(url)
    
    ## Check what was the last height of the page
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    ## Get the whole page data by loading all data from lazy loading page
    iteration = 0
    while True:
        
        iteration += 1
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
        time.sleep(SCROLL_PAUSE_TIME)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        if (new_height == last_height) or (iteration == params_iter['iter_threshold']):
            break
            
    return driver

def url_validator(url:str) -> bool:
    """
    
    Validates if the url have correct format
    ------------------------------------
    
    Input:
    url (str) : url string to be checked 
    
    ------------------------------------
    Output:
    Bool 
    
    """
    regex = re.compile(
            r'^(?:http|ftp)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
            r'localhost|' #localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
            r'(?::\d+)?' # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    if re.match(regex, url):
        return True
    
    return False

def get_industry_info(ticker,trial=0):
    
    if trial > 3:
        print('Failed for ticker => ',ticker)
        return ['','','','']
    
    if (ticker is None) or (ticker.strip() == ''):
        return ['','','','']
    
    industry_data_df = pd.read_csv('industry.csv')
    
    filtered_industry_data_df = industry_data_df[industry_data_df['ticker']==ticker]
    
    if filtered_industry_data_df.shape[0]>0:
        return [
            filtered_industry_data_df['Macro-Economic Sector'].iat[0],
            filtered_industry_data_df['Sector'].iat[0],
            filtered_industry_data_df['Industry'].iat[0],
            filtered_industry_data_df['Basic Industry'].iat[0]
        ]
    
    else:
        print(ticker)
        
        url = 'https://www.nseindia.com/get-quotes/equity?symbol={}'.format(ticker)

        params_iter = {}
        params_iter['scroll_wait_time'] = 5.0
        params_iter['iter_threshold'] = 1
        params_iter['silent_mode'] = True

        driver_data = get_driver_data(url , params_iter)

        page_content_str = driver_data.page_source
        bs4_soup_data_list = BeautifulSoup(page_content_str)

        driver_data.close()

        for table in bs4_soup_data_list.findAll('table'):
            if table.attrs.get('id','') == 'industryInfo':
                header = list(table.stripped_strings)[:4]
                body = list(table.stripped_strings)[4:]
                
                try:
                    industry_data_df.loc[len(industry_data_df)] = [ticker,'NSE',body[0],body[1],body[2],body[3]] 

                    industry_data_df.to_csv('industry.csv',index=False)
                except:
                    trial += 1
                    return get_industry_info(ticker,trial)
                
                return body
            
        return ['','','','']

def get_stock_historical_data(symbol,start_date,end_date):
    
    payload_df = pd.DataFrame()
    baseurl = "https://www.nseindia.com/"
    series = "EQ"
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
        'Sec-Fetch-User': '?1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
    }
    
    
    dt_start_date = datetime.datetime.strptime(start_date,"%d-%m-%Y")
    dt_end_date = datetime.datetime.strptime(end_date,"%d-%m-%Y")
    
    dt_inter_end_date = dt_start_date
    
    while (dt_end_date - dt_inter_end_date).days > 0:
        
        print("\r",dt_start_date," to " , dt_inter_end_date , " with days " ,abs((dt_inter_end_date - dt_start_date).days))
        
        dt_inter_end_date = dt_inter_end_date + datetime.timedelta(days=40)
        dt_inter_end_date = min([dt_inter_end_date,dt_end_date])
        
        inter_end_date = datetime.datetime.strftime(dt_inter_end_date,"%d-%m-%Y")
        
        url="https://www.nseindia.com/api/historical/cm/equity?symbol="+symbol+"&series=[%22"+series+"%22]&from="+str(start_date)+"&to="+str(inter_end_date)+""
    
        session = requests.Session()
        request = session.get(baseurl, headers=headers, timeout=5)
        cookies = dict(request.cookies)
        payload = session.get(url, headers=headers, timeout=5, cookies=cookies).json()

        inter_payload_df = pd.DataFrame(payload['data'])
        
        if payload_df.shape[0]:
            payload_df = pd.concat([payload_df,inter_payload_df])
        else:
            payload_df = inter_payload_df
        
    return payload_df

