# -*- coding: utf-8 -*-
"""
Created on Fri Sept 27  17:20:29 2024

@author: Pranav Belmannu
"""


import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import logging
import time
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File path for saving the DataFrame
output_file = 'distinct_urls_with_company_names.csv'
checkpoint_file = 'checkpoint.csv'

# Read data from Parquet files
urls = pd.read_parquet('urls.parquet', engine="pyarrow")
header_data = pd.read_parquet('header_data.parquet', engine="pyarrow")
distinct_urls = urls.drop_duplicates('url')

# Check if a checkpoint file exists and load it
if os.path.exists(checkpoint_file):
    checkpoint = pd.read_csv(checkpoint_file)
    distinct_urls = distinct_urls[~distinct_urls['url'].isin(checkpoint['url'])]  # Remove already processed URLs
else:
    checkpoint = pd.DataFrame(columns=['url', 'company_name', 'request_failed'])

# Function to remove common TLDs
def remove_tlds(domain_name, tlds):
    for tld in tlds:
        if domain_name.endswith(tld):
            domain_name = domain_name[:-len(tld)]
    return domain_name

# Function to extract company name from the HTML content
def extract_company_name(soup, url):
    parsed_url = urlparse(url)
    domain_name = parsed_url.netloc.replace('www.', '')

    # Remove common TLDs
    tlds = ['.com', '.net', '.org', '.io', '.co', '.info', '.biz']  # Add more TLDs as needed
    clean_domain_name = remove_tlds(domain_name, tlds)

    company_name = None

    # Check <title> tag for company name
    title_tag = soup.title
    if title_tag and title_tag.string:
        title_text = title_tag.string.strip()
        company_name = title_text.split('-')[0].split('|')[0].strip()

    # Check <meta> tag for "og:site_name" or "name" attributes
    meta_tags = soup.find_all('meta')
    for meta in meta_tags:
        if 'property' in meta.attrs and meta.attrs['property'].lower() == 'og:site_name' and 'content' in meta.attrs:
            company_name = meta.attrs['content'].strip()
        elif 'name' in meta.attrs and meta.attrs['name'].lower() == 'application-name' and 'content' in meta.attrs:
            company_name = meta.attrs['content'].strip()

    # Check common header tags for company name
    header_tags = soup.find_all(['h1', 'h2'])
    for header in header_tags:
        header_text = header.get_text(strip=True)
        if clean_domain_name.lower() in header_text.lower():
            company_name = header_text.strip()
            break

    # Verify the extracted company name against the domain name
    if company_name and clean_domain_name.lower() in company_name.lower():
        return company_name

    return None

# Function to process each URL
def process_url(index, url):
    https_url = f'https://{url}' if not url.startswith('http') else url
    http_url = f'http://{url}' if not url.startswith('https') else url
    company_name = None
    request_failed = False
    soup = None  # Initialize soup variable

    # Try fetching content with a retry mechanism
    for attempt in range(3):  # 3 attempts
        try:
            response = requests.get(https_url, allow_redirects=True, timeout=10)
            response.raise_for_status()
            logging.info(f"Success with HTTPS: {https_url}")

            soup = BeautifulSoup(response.content, 'html.parser')
            company_name = extract_company_name(soup, https_url)
            break  # Exit loop if successful

        except requests.exceptions.RequestException as e:
            logging.warning(f"HTTPS failed for {https_url}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff

            # Retry with HTTP
            try:
                response = requests.get(http_url, allow_redirects=True, timeout=10)
                response.raise_for_status()
                logging.info(f"Success with HTTP: {http_url}")

                soup = BeautifulSoup(response.content, 'html.parser')
                company_name = extract_company_name(soup, http_url)
                break  # Exit loop if successful

            except requests.exceptions.RequestException as e:
                logging.warning(f"HTTP also failed for {http_url}: {e}")
                request_failed = True  # Mark as failed if both attempts fail

    return index, company_name, request_failed, soup  # Return soup object as well

# Initialize lists to store results
indices = []
company_names = []
request_failures = []
soups = []  # List to store soup objects

# Use ThreadPoolExecutor to process URLs in parallel
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_url, idx, row['url']) for idx, row in distinct_urls.iterrows()]
    
    for future in as_completed(futures):
        index, company_name, request_failed, soup = future.result()
        indices.append(index)
        company_names.append(company_name)
        request_failures.append(request_failed)
        soups.append(soup)  # Append the soup object to the list

        # Update the DataFrame with results
        distinct_urls.at[index, 'company_name'] = company_name
        distinct_urls.at[index, 'request_failed'] = request_failed
        #distinct_urls.at[index, 'soup'] = soup  # Store the soup object

        # Save checkpoint after every 10 URLs processed
        if (len(indices) % 10) == 0:
            checkpoint = distinct_urls[['url', 'company_name', 'request_failed']].dropna()  # Save only processed rows
            checkpoint.to_csv(checkpoint_file, index=False)  # Save checkpoint

# Final save of the DataFrame
distinct_urls.to_csv(output_file, index=False)
logging.info("All URLs processed and results saved to the CSV file.")


# =============================================================================
# This is the Task 2 which adds and validates NAICS code for each URL if available
#Here I take each URL name explode i.e separate each word and compare it against
# the each business name. We probably will get NAICS code matches which are possible

# =============================================================================
# Check the resulting DataFrame
print(distinct_urls.head())

#Step 1 declare NAICS2 in header data as int and perform string replacement
# spaces and change lowercase
header_data['NAICS2']=header_data['NAICS2'].astype(int)
distinct_urls['NAICS'] = 'None'
header_data['new_business_name']=header_data.business_name.str.replace(" ","")
header_data['new_business_name']=header_data['new_business_name'].str.lower()


# Explode the lists in company_name_words so each word is a separate row
exploded = distinct_urls.explode('company_name_words')

# Step 2: Merge exploded data with header_data on the new_business_name
merged = exploded.merge(header_data, left_on='company_name_words', 
                        right_on='new_business_name', how='left')

# Step 3: Consolidate the results back into the original structure 
#by grouping by 'url'
# Take the first match for NAICS (you can adjust this if you need to handle
# multiple matches differently)
result = merged.groupby('url').agg({
    'NAICS2': 'first',  # Assuming one match is enough, take the first NAICS2 match
    'company_name': 'first'  # Restore the original company_name
}).reset_index()

# Step 4: Update distinct_url with the NAICS from result
distinct_urls['NAICS'] = distinct_urls['url'].map(result.set_index('url')['NAICS2'])

print(distinct_urls['NAICS'].value_counts())

# =============================================================================
# I have probably selected most basic ML model to analyze the data.My soup collection
# took 3 working days to complete and I had to optimize my code to get easiest 
# results
# =============================================================================


# # Task 3: Machine learning

from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
import numpy as np


distinct_urls_cleaned = distinct_urls.dropna(subset=['company_name', 'NAICS'])
distinct_urls_cleaned = distinct_urls_cleaned.reset_index(drop=True)
distinct_urls_cleaned.isna().value_counts()

url_dict=distinct_urls_cleaned[['company_name','NAICS']].to_dict(orient='records')

vectorizer = DictVectorizer()

X=vectorizer.fit_transform(url_dict).toarray()


y=distinct_urls_cleaned['NAICS']

vectorizer.get_feature_names_out()

X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2)


# Step 2: Convert X_train, X_test, y_train, y_test into DataFrames
train_data = pd.concat([X_train, y_train], axis=1)  
# Combine features and target for the training set
test_data = pd.concat([X_test, y_test], axis=1)   
 # Combine features and target for the test set

# Step 3: Save the training and test sets to separate files
train_file = 'train_data.csv'
test_file = 'test_data.csv'

train_data.to_csv(train_file, index=False)  # Save training set
test_data.to_csv(test_file, index=False)    # Save test set




svm_regressor = SVR()
svm_regressor.fit(X_train, y_train)

y_pred = svm_regressor.predict(X_test)


from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"Root Mean Squared Error (RMSE): {rmse}")
print(f"R² Score: {r2}")


# I see that R2 value is 46% I can see couple reasons why

# My final dataset had only 62 values.
# I might have not clean extraction of Company names. 
#Although, I had some checkpoints but I managed to bring in sentences 
#within the dataset.

