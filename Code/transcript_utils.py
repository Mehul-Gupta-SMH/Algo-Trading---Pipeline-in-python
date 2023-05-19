#!/usr/bin/env python
# coding: utf-8

# ## Importing Data Processing Libs

# In[ ]:


import re
import requests


# ## Function Definition

# In[ ]:


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
        ticker_cd = transcript_header[transcript_header.find(r'(')+1 : transcript_header.find(r')')]
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

