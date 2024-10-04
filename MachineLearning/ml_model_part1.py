# -*- coding: utf-8 -*-
"""
Created on Fri Oct  4 17:54:54 2024

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
        distinct_urls.at[index, 'soup'] = soup  # Store the soup object

        # Save checkpoint after every 10 URLs processed
        if (len(indices) % 10) == 0:
            checkpoint = distinct_urls[['url', 'company_name', 'request_failed']].dropna()  # Save only processed rows
            checkpoint.to_csv(checkpoint_file, index=False)  # Save checkpoint

# Final save of the DataFrame
distinct_urls.to_csv(output_file, index=False)
logging.info("All URLs processed and results saved to the CSV file.")
