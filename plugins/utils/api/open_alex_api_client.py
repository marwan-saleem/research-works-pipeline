import json
import requests as req


class openAlexApiClient():
    '''Client to access open alex resources'''

    def get_works_search(self, from_date, search_text):
        '''Get complete JSON of work entities for searched text from specified publication date'''
        
        page_num = 1
        num_per_page = 200
        loop_cond = True
        total_results = []
        search_text = search_text.replace(' ', '+')
        while loop_cond:
            print(page_num)
            req_url = f'https://api.openalex.org/works?page={page_num}&per_page={num_per_page}&filter=from_publication_date:{from_date},default.search:{search_text}'      
            response = req.get(req_url)
            print(response.elapsed.total_seconds())
            res_json = response.json()
            work_count = res_json['meta']['count']
            total_results.extend(res_json['results'])
            print(len(res_json['results']), len(total_results), work_count)
            if len(total_results)>=work_count:
                loop_cond = False
            else:
                page_num+=1
        return total_results

    def get_works(self, from_date):
        ''' Get JSON of all works from the specified publication data '''
        
        page_num = 1
        num_per_page = 200
        loop_cond = True
        total_results = []
        while loop_cond:
            print(page_num)
            req_url = f'https://api.openalex.org/works?page={page_num}&per_page={num_per_page}&filter=from_publication_date:{from_date}'
            response = req.get(req_url)
            res_json = response.json()
            work_count = res_json['meta']['count']
            total_results.extend(res_json['results'])
            print(len(res_json['results']), len(total_results), work_count)
            if len(total_results)>=work_count:
                loop_cond = False
            else:
                page_num+=1
        return total_results