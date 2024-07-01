from datetime import datetime
import json

import numpy as np
import pandas as pd
import pytz
from utils.database.schemas import keywords_schema
from utils.database.table_model import tableModel


class keywordsTable(tableModel):
    '''Keywords database table object'''

    def __init__(self, table_json_path=None, raw_json_path=None):
        
        super().__init__(keywords_schema)
        self.table_json_path = table_json_path
        self.raw_json_path = raw_json_path
        # Based on processed or raw file being provided, table_json getting populated
        if self.table_json_path:
            self.read_table_json(self.table_json_path)
        elif self.raw_json_path:
            self.raw_to_df()
        else:
            raise Exception('Provide either table_json_path or raw_json_path to initialize the object')
    
    def raw_to_df(self):
        '''Transformation logic of JSON to DF'''

        json_obj = self._read_json_file(self.raw_json_path)
        req_fields = ['id', 'keywords']
        set_fields = set(req_fields)
        # Creating dataframe with required fields, if field absent then handling in exception
        try:
            df_keywords = pd.DataFrame(json_obj)[req_fields]
        except KeyError as e:
            err_str = str(e)
            # finding set of missing fields, and assigning NaN values for them
            list_end = err_str.find(']')
            missing_fields = err_str[2:list_end].replace("'", '').split(",")
            missing_set = set(missing_fields)
            avail_fields = list(set_fields.difference(missing_set))
            df_keywords = pd.DataFrame(json_obj)[avail_fields]
            df_keywords[missing_fields] = np.nan
        # Flattening list of JSONs, maintaining index from explode to assist concat()
        df_keywords = df_keywords.explode('keywords').dropna(subset=['keywords'])
        new_index = df_keywords.index
        df_flat_dict = pd.json_normalize(df_keywords['keywords'], sep='_').set_index(new_index, drop = True)
        df_flat_dict.columns = ['keyword_'+col  for col in df_flat_dict.columns]
        # Concat flattened records DF with main DF. The "new_index" will concat with the correct "id" records
        df_keywords = pd.concat([df_keywords.drop(['keywords'], axis=1), df_flat_dict], axis=1)
        df_keywords = df_keywords.rename({'id':'work_id'}, axis=1)
        df_keywords['pipeline_date'] = datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df_keywords.drop_duplicates(subset=['work_id', 'keyword_id'], keep='first', inplace=True)
        # Many keywords to One Id relation, adding seq_no to keywords as per relevance "score"
        df_keywords['seq_no'] = df_keywords.groupby('work_id')['keyword_score'].rank('first', ascending=False).astype('int64')
        self.table_df = df_keywords
        self.table_json = json.loads(df_keywords.to_json(orient='records'))