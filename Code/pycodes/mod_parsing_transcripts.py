#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import warnings
warnings.filterwarnings("ignore")


# ## Importing Selenium and Drivers

# In[ ]:


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.chrome.options import Options


# ## Importing Data Processing Libs

# In[ ]:


from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import requests
import time
import json
import datetime


# ## Import User-Defined Modules

# In[ ]:


from pycodes.mod_utility import *


# ## Creating User-Defined Functions

# In[ ]:


def get_ticker(transcript_header:str) -> str:
    """
    
    Parses out the ticker symbol from the transcript's header 
    ------------------------------------
    
    Input:
    transcript_header (str) : header of the transcripts 
    
    ------------------------------------
    Output:
    ticker_cd (str) : Ticker Symbol Code 
    
    """    
    try:
        ticker_cd = transcript_header[transcript_header.rfind(r'(')+1 : transcript_header.rfind(r')')]
        return ticker_cd
    except:
        return ''


# In[ ]:


def get_metadata(company_name:str):
    """
    
    Get ticker symbol from Company Name
    ------------------------------------
    
    Input:
    company_name (str) : Company Name
    
    ------------------------------------
    Output:
    company_code (str) : Company code from Yahoo Finance  
    
    """    
    
    yfinance = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    params = {"q": company_name, "quotes_count": 1, "country": "United States"}

    
    res = requests.get(url=yfinance, params=params, headers={'User-Agent': user_agent})
    data = res.json()

    try:
        meta_data = data['quotes'][0]
        exchange = meta_data['exchange']
        sector = meta_data['sector']
        industry = meta_data['industry']
    
    except:
        exchange = ''
        sector = ''
        industry = ''
    
    return [ exchange , sector , industry]


# In[ ]:


def extract_urls(driver_data,params_bs4_filter : dict) -> list:
    """
    
    Extract embedded urls from the main page in form of [header,link] pairs
    ------------------------------------
    
    Input:
    driver_data (Selenium.webdriver.Chrome object) : Contains data about the page that needs to be parsed 
    params_bs4_filter (dict) : Dictionary of parameters that are required for effective parsing using BeatifulSoup
    
    ------------------------------------
    Output:
    parsed_links_list (list) : List of [header,link] for embedded urls in the main page
    
    """      
    
    page_content_str = None
    bs4_soup_data_list = None
    parsed_links_list = []
    
    page_content_str = driver_data.page_source
    bs4_soup_data_list = BeautifulSoup(page_content_str)
    
    for links in bs4_soup_data_list.findAll(params_bs4_filter['name'],
                                            href=params_bs4_filter['href'], 
                                            attrs=params_bs4_filter['attrs'],
                                            recursive=params_bs4_filter['recursive']):
        link = links['href']
        header = links.contents[0]
        
        parsed_links_list.append([header,link])
    
    return parsed_links_list


# In[ ]:


def parse_page_data_for_url(driver ,params_bs4_filter : dict , params_iter : dict):
    """
    
    Extract embedded urls from the main page in form of Pandas Dataframe
    ------------------------------------
    
    Input:
    url (str) : url from which data is to be parsed 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    params_bs4_filter (dict) : Dictionary of parameters that are required for effective parsing using BeatifulSoup
    
    ------------------------------------
    Output:
    extracted_url_df (Pandas Dataframe) : Pandas Dataframe with header and corresponding urls
    
    """
    
    pre_processed_transcripts = pd.read_csv("extracted_transcripts.csv")
    
    ## Get [header,links] pairs for embedded urls 
    new_results = extract_urls(driver,params_bs4_filter)
    
    for transcripts_header in new_results:
        if transcripts_header[0] in list(pre_processed_transcripts['header']):
            new_results.remove(transcripts_header)
    
    ## Convert [header,links] pairs to pandas dataframe
    extracted_url_df = pd.DataFrame(new_results,columns=['header','link'])
    
    extracted_url_df[['Org Name','temp']] = extracted_url_df['header'].str.split('(', 1,expand=True)
    
    extracted_url_df['ticker_cd'] = extracted_url_df['header'].map(get_ticker).to_list()
    
    extracted_url_df[['stock_exchange','sector','industry']] = extracted_url_df['Org Name'].map(get_metadata).to_list()
    
    extracted_url_df[['nse_mes','nse_sector','nse_industry','nse_basic_industry']] = extracted_url_df['ticker_cd'].map(get_industry_info).to_list()
        
    
    ## Data cleaning:
    ## 1. Remove unwanted rows
    extracted_url_df = extracted_url_df[~extracted_url_df['header'].str.contains("\[",na=True)]
    extracted_url_df.reset_index(inplace = True)
    extracted_url_df.drop(['index','temp'],axis=1,inplace=True)
    
    ## 2. Remove duplicate rows
    extracted_url_df.drop_duplicates(inplace=True)
    
    return extracted_url_df


# In[ ]:


def extract_entity_participants(driver_data , transcript_header : str):
    """
    
    Extract participants from the transcripts
    ------------------------------------
    
    Input:
    driver_data (Selenium.webdriver.Chrome object) : Contains data about the page that needs to be parsed 
    transcript_header (str) : Company name to which the transcripts belongs to
    
    ------------------------------------
    Output:
    parsed_links_list (list) : List of ppts data [[],...]
    
    """     
    parsed_corp_ppts_list = []
    parsed_ppts_list = []
    page_content_str = None
    bs4_soup_data_list = None
    parsed_links_list = []
    
    page_content_str = driver_data.page_source
    bs4_soup_data_list = BeautifulSoup(page_content_str)
    
    
    ## For Corporate PPTs
    
    params_corp_ppts = {
        'tag_val' : 'h2',
        'text_val' : 'Corporate Participants:'
    }
    
    target = bs4_soup_data_list.find(params_corp_ppts['tag_val'],
                                     text=params_corp_ppts['text_val'])

    for sib in target.find_next_siblings():
        if sib.name==params_corp_ppts['tag_val']:
            break
        else:
            ppt_corp = transcript_header.split('(', 1)[0]
            try:
                ppt_name , ppt_desig = sib.text.replace("\xa0","").split("—")
            except:
                ppt_name , ppt_desig = sib.text.split("\xa0")

            parsed_corp_ppts_list.append([transcript_header , ppt_name , ppt_desig , ppt_corp])
    
          
    ## For Analysts
    
    params_analyst_ppts = {
        'tag_val' : 'h2',
        'text_val' : 'Analysts:'
    }
    
    target = bs4_soup_data_list.find(params_analyst_ppts['tag_val'],
                                     text=params_analyst_ppts['text_val'])
    

    for sib in target.find_next_siblings():
        if sib.name==params_analyst_ppts['tag_val']:
            break
        else:
            try:
                ppt_name , ppt_corp , ppt_desig = sib.text.replace("\xa0","").split("—")
            except:
                ppt_name , ppt_corp , ppt_desig = "Unidentified" , "" , "Analyst"
            
            parsed_ppts_list.append([transcript_header , ppt_name , ppt_desig , ppt_corp])
                
                
            
    
    
    return parsed_corp_ppts_list,parsed_ppts_list


# In[ ]:


def get_transcripts_urls_from_url(url:str,params_iter : dict,params_bs4_filter : dict):
    """
    
    Extract url of transcripts from main url
    ------------------------------------
    
    Input:
    url (str) : url from which data is to be parsed 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    params_bs4_filter (dict) : Dictionary of parameters that are required for effective parsing using BeatifulSoup
    
    ------------------------------------
    Output:
    extracted_url_df (pandas dataframe) : Dataframe of extrated urls
    
    """   
    
    sel_driver = get_driver_data(url , params_iter)
    
    extracted_url_df = parse_page_data_for_url(sel_driver , params_bs4_filter , params_iter)
    
    return extracted_url_df , sel_driver


# In[ ]:


def get_disclosures_from_transcripts(driver_data,pading_cols:dict):
    
    page_content_str = driver_data.page_source
    bs4_soup_data_list = BeautifulSoup(page_content_str)
    
    header = None

    value_list = []

    for values in bs4_soup_data_list.find('h2',text='Presentation:').find_next_siblings():

        if values.find('span') is not None:
            continue

        if values.find('strong') is not None:
            header = values.text

        else:
            value_list.append([header,values.text])

    data_df = pd.DataFrame(value_list,columns=['said_by','info'])
    
    data_df[list(pading_cols.keys())] = list(pading_cols.values())
    
    return data_df


# In[ ]:


def get_question_answers_from_transcripts(driver_data,pading_cols:dict):
    
    page_content_str = driver_data.page_source
    bs4_soup_data_list = BeautifulSoup(page_content_str)    

    header = None

    question = []

    value_list = []

    for values in bs4_soup_data_list.find('h2',text='Questions and Answers:').find_next_siblings():

        if values.find('span') is not None:
            continue

        if values.find('strong') is not None:
            header = values.text

        else:
            if header == 'Operator':
                continue
            if 'Analyst' in header:
                question = [header,values.text]
            else:
                value_list.append([question[0],question[1],header,values.text])

    data_df = pd.DataFrame(value_list,columns=['question_by','question','answer_by','answer'])
    
    data_df[list(pading_cols.keys())] = list(pading_cols.values())
    
    return data_df


# In[ ]:


def get_data_from_transcripts(extracted_url_df : pd.DataFrame , sel_driver, params_debug):
    """
    
    Extract data from transcripts from url dataframe
    1. Participants Data
        a. Corporate Participants
        b. Analysts Participants
    ------------------------------------
    
    Input:
    url (str) : url from which data is to be parsed 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    params_bs4_filter (dict) : Dictionary of parameters that are required for effective parsing using BeatifulSoup
    
    ------------------------------------
    Output:
    corp_ppts_df (pandas dataframe) : Dataframe of Corporate Participants
    analyst_ppts_df (pandas dataframe) : Dataframe of Analysts Participants
    
    """ 
    
    corp_ppts_df = pd.DataFrame()
    analyst_ppts_df = pd.DataFrame()
    disclosures_df = pd.DataFrame()
    qa_df = pd.DataFrame()
    
    for index,row in extracted_url_df.iterrows():
        
        if params_debug.get('debug_mode',False):
            if params_debug.get('ticker',None) == row['ticker_cd']:
                run_cd = True
                show_inter_results = True
            else:
                run_cd = False
                show_inter_results = False
        else:
            run_cd = True
            show_inter_results = False
        
        if run_cd:
            try:

                print('Information Retrieval Started -> ',row['Org Name'],'(',row['ticker_cd'],')')
                sel_driver.get(row['link'])

                pading_cols = {
                    "ticker_cd" : row['ticker_cd'],
                    "Org Name" : row['Org Name']
                }

                print('\n')

                print('\tStep 1 : Get Disclosures data','\n')
                inter_disclosures_df = get_disclosures_from_transcripts(sel_driver,pading_cols)
                
                if show_inter_results:
                    print("\n=====================\n Intermediate Data \n=====================\n")
                    print(inter_disclosures_df)

                    
                print('\tStep 2 : Get Q/A data','\n')
                inter_qa_df = get_question_answers_from_transcripts(sel_driver,pading_cols)
                
                if show_inter_results:
                    print("\n=====================\n Intermediate Data \n=====================\n")
                    print(inter_qa_df)

                    
                print('\tStep 3 : Get participants data','\n')
                corp_ppts_list , analyst_ppts_list = extract_entity_participants(sel_driver,row['header'])
                
                if show_inter_results:
                    print("\n=====================\n Intermediate Data List \n=====================\n")
                    print(corp_ppts_list,"\n*****\n")
                    print(analyst_ppts_list,"\n*****\n")

                inter_corp_ppts_df = pd.DataFrame(corp_ppts_list,columns=['Transcript Header','Name','Designation','Corp Name'])

                inter_analyst_ppts_df = pd.DataFrame(analyst_ppts_list,columns=['Transcript Header','Name','Designation','Corp Name'])

                if show_inter_results:
                    print("\n=====================\n Intermediate Data \n=====================\n")
                    print(inter_corp_ppts_df,"\n*****\n")
                    print(inter_analyst_ppts_df,"\n*****\n")


                print('\tStep 4 : Combine Data','\n')
                
                if corp_ppts_df.shape[1] > 0:
                    corp_ppts_df = pd.concat([corp_ppts_df,inter_corp_ppts_df])
                else:
                    corp_ppts_df = inter_corp_ppts_df
                
                if analyst_ppts_df.shape[1] > 0:
                    analyst_ppts_df = pd.concat([analyst_ppts_df,inter_analyst_ppts_df])
                else:
                    analyst_ppts_df = inter_analyst_ppts_df
                    
                if disclosures_df.shape[1] > 0:
                    disclosures_df = pd.concat([disclosures_df,inter_disclosures_df])
                else:
                    disclosures_df = inter_disclosures_df      
                    
                if qa_df.shape[1] > 0:
                    qa_df = pd.concat([qa_df,inter_qa_df])
                else:
                    qa_df = inter_qa_df

            except Exception as E:
                print('\r','Information Retrieval Failed -> ',row['Org Name'],'(',row['ticker_cd'],')\n')
                print("\r","Unable to open transcript","\n")
#                 print("Error =", str(E))
        
    return corp_ppts_df , analyst_ppts_df , disclosures_df , qa_df


# In[ ]:


def get_transcripts_data_wrapper(url:str,params_iter : dict,params_bs4_filter : dict, params_debug : dict):
    """
    
    Extract data from transcripts from url 
    ------------------------------------
    
    Input:
    url (str) : url from which data is to be parsed 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    params_bs4_filter (dict) : Dictionary of parameters that are required for effective parsing using BeatifulSoup
    
    ------------------------------------
    Output:
    corp_ppts_df (pandas dataframe) : Dataframe of Corporate Participants
    analyst_ppts_df (pandas dataframe) : Dataframe of Analysts Participants
    
    """
    
    extracted_url_df , sel_driver = get_transcripts_urls_from_url(url , params_iter , params_bs4_filter)
    
    corp_ppts_df , analyst_ppts_df , disclosures_df , qa_df = get_data_from_transcripts(extracted_url_df , sel_driver, params_debug)
    
    extracted_url_df.to_csv("extracted_transcripts.csv",mode='a',index=False)
    corp_ppts_df.to_csv("corporate_participants.csv",mode='a',index=False)
    analyst_ppts_df.to_csv("analysts_participants.csv",mode='a',index=False)
    disclosures_df.to_csv("disclosures_texts.csv",mode='a',index=False)
    qa_df.to_csv("qanda_textx.csv",mode='a',index=False)
        
    return extracted_url_df , corp_ppts_df , analyst_ppts_df , disclosures_df , qa_df
    

