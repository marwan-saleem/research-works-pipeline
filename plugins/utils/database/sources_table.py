from datetime import datetime
import json

import numpy as np
import pandas as pd
import pytz
from utils.database.schemas import sources_schema
from utils.database.table_model import tableModel


class sourcesTable(tableModel):
    '''Sources database table object'''

    def __init__(self, table_json_path=None, raw_json_path=None):
        
        super().__init__(sources_schema)
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
        req_fields = ['primary_location']
        set_fields = set(req_fields)
        # Creating dataframe with required fields, if field absent then handling in exception
        try:
            df_source = pd.DataFrame(json_obj)[req_fields]
        except KeyError as e:
            err_str = str(e)
            # finding set of missing fields, and assigning NaN values for them
            list_end = err_str.find(']')
            missing_fields = err_str[2:list_end].replace("'", '').split(",")
            missing_set = set(missing_fields)
            avail_fields = list(set_fields.difference(missing_set))
            df_source = pd.DataFrame(json_obj['results'])[avail_fields]
            df_source[missing_fields] = np.nan
        # Flattening column with JSON records
        df_source = pd.json_normalize(df_source['primary_location'].dropna(), sep='_')
        df_source = df_source.loc[:,['source_id', 'source_display_name', 'is_oa']]
        df_source['pipeline_date'] = datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df_source.drop_duplicates(subset=['source_id'], keep='first',inplace=True)
        df_source.dropna(subset=['source_id'], inplace=True)
        self.table_df = df_source
        self.table_json = json.loads(df_source.to_json(orient='records'))
