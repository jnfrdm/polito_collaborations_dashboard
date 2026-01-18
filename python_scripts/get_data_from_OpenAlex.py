ROR_POLITO = 'https://ror.org/00bgk9508'
work_type = 'dataset'
import json
import os
import time
import requests
import re
import math

per_page = 25

# First, get the first page to know the total count
page_number = 1
get_all_dataset_works_call = f"https://api.openalex.org/works?&filter=type:{work_type},authorships.institutions.ror:{ROR_POLITO}&page={page_number}&per_page={per_page}"       
print(f"Fetching page {page_number}...")
print(get_all_dataset_works_call)
response = requests.get(get_all_dataset_works_call) # Get the response from the API for the given page number and per page limit    

if response.status_code == 200: # If the response is successful
    response.raise_for_status() # Raise an error if the response is not successful
    results_OA_dataset_works = response.json() # Get the JSON response from the API
    total_count = results_OA_dataset_works['meta']['count']
    print(f"Total Polito dataset works: {total_count}") # Print the total number of Polito dataset works
    
    # Collect all results
    all_results = results_OA_dataset_works['results']
    
    # Calculate total pages needed
    total_pages = math.ceil(total_count / per_page)
    print(f"Total pages to fetch: {total_pages}")
    
    # Fetch remaining pages
    for page_number in range(2, total_pages + 1):
        get_all_dataset_works_call = f"https://api.openalex.org/works?&filter=type:{work_type},authorships.institutions.ror:{ROR_POLITO}&page={page_number}&per_page={per_page}"
        print(f"Fetching page {page_number}/{total_pages}...")
        response = requests.get(get_all_dataset_works_call)
        
        if response.status_code == 200:
            response.raise_for_status()
            page_results = response.json()
            all_results.extend(page_results['results'])
            print(f"  Retrieved {len(page_results['results'])} works from page {page_number}")
        else:
            print(f"  Warning: Failed to fetch page {page_number} (status code: {response.status_code})")
        
        # Be gentle with the API - add a small delay between requests
        time.sleep(0.1)
    
    # Write all results to file
    print(f"\nWriting {len(all_results)} total works to data/polito_works.json...")
    with open('data/polito_works.json', 'w') as f: # Write the results to a file
        json.dump(all_results, f, indent=2)   # Write the results to a file in the data folder
    print("Done!")
else:
    print(f"Error: Failed to fetch data (status code: {response.status_code})")
