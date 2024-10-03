# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 12:18:53 2024

@author: Pranav Belmannu
"""
import pandas as pd

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
            
    except requests.exceptions.SSLError as ssl_error:
        print(f"SSL error occurred for {full_url}: {ssl_error}")
        data_to_label.loc[index, 'match'] = "SSL Error"
        
    except requests.exceptions.Timeout as timeout_error:
        print(f"Timeout error occurred for {full_url}: {timeout_error}")
        data_to_label.loc[index, 'match'] = "Timeout Error"
        
    except requests.exceptions.ConnectionError as conn_error:
        print(f"Connection error occurred for {full_url}: {conn_error}")
        data_to_label.loc[index, 'match'] = "Connection Error"
        
    except requests.exceptions.RequestException as req_error:
        print(f"General request error occurred for {full_url}: {req_error}")
        data_to_label.loc[index, 'match'] = "Request Error"
        
    except Exception as e:
        print(f"An unexpected error occurred for {full_url}: {e}")
        data_to_label.loc[index, 'match'] = "Unexpected Error"

# Optional: Save the updated DataFrame to a file to review results later
data_to_label.to_csv("labeled_data.csv", index=False)