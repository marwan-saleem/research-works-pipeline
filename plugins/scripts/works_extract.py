from datetime import datetime, timedelta
import json
import os
import sys

import requests as req
from utils.api.open_alex_api_client import openAlexApiClient

def works_extract():
    
    #fetching JSON data for all the pages in result for search of phrase "artificial intelligence"
    client = openAlexApiClient()
    # fetching from date 2 days before current date
    from_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    search_text = 'artificial intelligence'
    total_results = client.get_works_search(from_date, search_text)
    # archiving complete JSON from result 
    result_json_str = json.dumps(total_results)
    with open('/opt/airflow/archive/api_response_works.json', 'w') as file:
        file.write(result_json_str)