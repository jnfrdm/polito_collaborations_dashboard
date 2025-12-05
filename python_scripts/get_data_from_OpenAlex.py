ROR_POLITO = 'https://ror.org/00bgk9508'
work_type = 'dataset'
import json
import os
import time
import requests
import re
import math

page_number = 543
per_page = 25

get_all_dataset_works_call = f"https://api.openalex.org/works?&filter=type:{work_type},authorships.institutions.ror:{ROR_POLITO}"
print(get_all_dataset_works_call)
response = requests.get(get_all_dataset_works_call)

if response.status_code == 200:
    response.raise_for_status()
    results_OA_dataset_works = response.json()
    print(f"Total Polito dataset works: {results_OA_dataset_works['meta']['count']}")
    with open('data/polito_works.json', 'w') as f:
        json.dump(results_OA_dataset_works['results'], f)  
