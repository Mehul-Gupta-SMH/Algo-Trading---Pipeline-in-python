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


# ## Import User Defined Modules

# In[ ]:


from pycodes.mod_utility import *


# ## Create User Defined Functions

# In[ ]:


def clean_fundamentals_table_data(my_list:list) -> list:
    """
    
    Clean Parsed table data for fundamentals
    ------------------------------------
    
    Input:
    my_list (list) : Table row to be cleaned
    
    ------------------------------------
    Output:
    my_list (list) : Cleaned Row
    
    """    
    
    my_list = list(map(str.strip, my_list))
    
    strings_to_clean = ['' ,
                        '        ' ,
                        '          ']
    
    for string in strings_to_clean:
        try:
            while True:
                my_list.remove(string)
        except ValueError:
            pass
    
    
    my_list = list(map(lambda x: x.replace(",",""), my_list))
    
    my_list = list(map(lambda x: x.replace("+",""), my_list))
    
    return my_list


# In[ ]:


def get_fundamentals_data_tables(bs4_soup_data_list , params_fndmntls_data:dict , pading_cols:dict):
    
    data_section_tag = params_fndmntls_data.get('data_section_tag','section')
    data_section_tag_id = params_fndmntls_data.get('data_section_tag_id','id')
    
    table_section_tag = params_fndmntls_data.get('table_section_tag','table')
    table_section_subtag = params_fndmntls_data.get('table_section_subtag','class')

    parsed_table_df = pd.DataFrame()
    
    for tables in bs4_soup_data_list.findAll(data_section_tag):
        
        parsed_table_inter_df = pd.DataFrame()
        
        for table in tables.findAll(table_section_tag):
            
            print('\t',tables.get(data_section_tag_id,'')," -> ",'Data Parsing - Started')
            
            rows = []
            header = []
            values = []

            for row in table.findAll("tr"):
                values.append(row.text.split("\n"))
            
            values = list(map(clean_fundamentals_table_data,values))
            
            header = ['Metric'] + values[0]

            rows = values[1:]

            try:
                parsed_table_inter_df = pd.DataFrame(rows,columns=header)
                
                parsed_table_inter_df = pd.melt(
                        parsed_table_inter_df, 
                        id_vars =list(parsed_table_inter_df.columns)[0], 
                        value_vars =list(parsed_table_inter_df.columns)[1:]
                       )

                parsed_table_inter_df['fundamental_data_type'] = tables.get(data_section_tag_id,'')
                
                
                if parsed_table_df.shape[0]:
                    parsed_table_df = pd.concat(parsed_table_inter_df,parsed_table_df)
                else:
                    parsed_table_df = parsed_table_inter_df
                
                print('\r\t',tables.get(data_section_tag_id,'')," -> ",'Data Parsing - Success\n')
                
            except:
                print('\r\t',tables.get(data_section_tag_id,'')," -> ",'Data Parsing - Failed\n')
    
    parsed_table_df[list(pading_cols.keys())] = list(pading_cols.values())
    
    return parsed_table_df


# In[ ]:


def get_fundamentals_data_wrapper(extracted_url_df,params_iter):
    """
    
    Get Funadamentals data for the identified ticker 
    ------------------------------------
    
    Input:
    params_fundamentals (dict) : parameter for scraping fundamentals data 
    params_iter (dict) : Dictionary of parameters that are required to iterate over the web page
    
    ------------------------------------
    Output:
    
    """    
    
    fundamentals_data_df = pd.DataFrame()
    
    for index,stocks in extracted_url_df.iterrows():
        
        print('Fundamantals Data -> ',stocks['Org Name'],'(',stocks['ticker_cd'],')\n')
    
        url = "https://www.screener.in/company/{}/consolidated/#profit-loss".format(stocks['ticker_cd'])

        driver_data = get_driver_data(url , params_iter)

        page_content_str = driver_data.page_source
        bs4_soup_data_list = BeautifulSoup(page_content_str)
        
        driver_data.close()

        params_fndmntls_data = {
            'data_section_tag':'section' ,
            'data_section_tag_id':'id' ,
            'table_section_tag':'table' ,
            'table_section_subtag':'class' ,
        }

        pading_cols = {
            "ticker_cd" : stocks['ticker_cd'],
            "Org Name" : stocks['Org Name']
        }

        inter_fundamentals_data_df = pd.DataFrame()
        inter_fundamentals_data_df = get_fundamentals_data_tables(bs4_soup_data_list,params_fndmntls_data,pading_cols)
        
        if not inter_fundamentals_data_df.shape[0]:
            pass
        elif fundamentals_data_df.shape[0]:
            fundamentals_data_df = pd.concat([inter_fundamentals_data_df,fundamentals_data_df])
        else:
            fundamentals_data_df = inter_fundamentals_data_df
            
    fundamentals_data_df = fundamentals_data_df.dropna().reset_index()
    
    fundamentals_data_df.drop(['index'],axis=1,inplace=True)
    
    return fundamentals_data_df

