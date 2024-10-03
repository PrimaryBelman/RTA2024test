# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 12:18:53 2024

@author: Pranav Belmannu
"""
import pandas as pd
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore")

data_to_label=pd.read_csv("data_to_label.csv")

import requests
from bs4 import BeautifulSoup as bs


for index in range(10000,10500):
    https='https://'
    full_url=https+data_to_label.loc[index,'url']
    try:
        response=requests.get(full_url,verify=False,timeout=20,stream=True)

        if response.status_code!=200:
            data_to_label.loc[index,'redirected_url']=full_url
        else:
            data_to_label.loc[index,'redirected_url']=False
            soup=bs(response.content,'html.parser')
            elements=soup.find_all(string=True)
    
        match_found=False
        for element in elements:
            if (data_to_label.loc[index,'business_name'].lower() in element.lower()):
                data_to_label.loc[index,'match']=True
                match_found=True
                break
        
        if not match_found:
            data_to_label.loc[index,'match']=False
            #print(data_to_label.loc[index])
            print(index)
    except requests.exceptions.SSLError as ssl_error:
        print(f"SSL error occurred for {full_url}: {ssl_error}")
        
        
    except requests.exceptions.Timeout as timeout_error:
        print(f"Timeout error occurred for {full_url}: {timeout_error}")
        
        
    except requests.exceptions.ConnectionError as conn_error:
        print(f"Connection error occurred for {full_url}: {conn_error}")
        
        
    except requests.exceptions.RequestException as req_error:
        print(f"General request error occurred for {full_url}: {req_error}")
        
        
    except Exception as e:
        print(f"An unexpected error occurred for {full_url}: {e}")
        

data_to_label.to_csv("labeled_data.csv")