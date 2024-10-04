# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 12:18:53 2024

@author: Pranav Belmannu
"""

import pandas as pd
data_to_label=pd.read_csv("data_to_label.csv")
import requests
from bs4 import BeautifulSoup as bs

# Iterate over rows from index 10,000 to 10,500
for idx, row in data_to_label.iloc[10000:10500].iterrows():
    https = 'https://'
    http = 'http://'

# =============================================================================
# I do add https first to check the url and if it fails I add http.
# =============================================================================

    
    # Construct the initial HTTPS URL
    full_url = https + row['url']

    def make_request(url):
        try:
            response = requests.get(url, verify=False, timeout=20, stream=True)
            return response
        except requests.exceptions.RequestException as req_error:
            print(f"General request error occurred for {url}: {req_error}")
            return None

    # Try HTTPS first
    response = make_request(full_url)

    if not response:  # If the HTTPS request failed
        # Construct the HTTP URL
        full_url = http + row['url']
        response = make_request(full_url)

# =============================================================================
# If the response code is 3XX, it means the url is redirected therefore the
# url is saved
# =============================================================================


    if response:  # If we received a response
        if response.history:  # Check if there was a redirect
            # Check the final status code of the last request in the history
            final_response = response.history[-1]
            if final_response.status_code in {301, 302, 303, 307, 308}:  # Common redirect status codes
                data_to_label.loc[row.name, 'redirected_url'] = final_response.url
            else:
                data_to_label.loc[row.name, 'redirected_url'] = False
        else:
            data_to_label.loc[row.name, 'redirected_url'] = False

        soup = bs(response.content, 'html.parser')
        elements = soup.find_all(string=True)

        match_found = False
        for element in elements:
            if row['business_name'].lower() in element.lower():
                data_to_label.loc[row.name, 'match'] = True
                match_found = True
                break

        if not match_found:
            data_to_label.loc[row.name, 'match'] = False
            print(row.name)

    else:
        print(f"Failed to retrieve URL: {full_url}")

data_to_label.to_csv("labeled_data.csv")