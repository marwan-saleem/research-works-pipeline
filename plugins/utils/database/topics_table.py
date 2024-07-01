from datetime import datetime
import json

import numpy as np
import pandas as pd
import pytz
from utils.database.schemas import topics_schema
from utils.database.table_model import tableModel


class topicsTable(tableModel):
    '''Topics database table object'''

    def __init__(self, table_json_path=None, raw_json_path=None):
        
        super().__init__(topics_schema)
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
        req_fields = ['id', 'topics']
        set_fields = set(req_fields)
        # Creating dataframe with required fields, if field absent then handling in exception
        try:
            df_topics = pd.DataFrame(json_obj)[req_fields]
        except KeyError as e:
            err_str = str(e)
            # finding set of missing fields, and assigning NaN values for them
            list_end = err_str.find(']')
            missing_fields = err_str[2:list_end].replace("'", '').split(",")
            missing_set = set(missing_fields)
            avail_fields = list(set_fields.difference(missing_set))
            df_topics = pd.DataFrame(json_obj['results'])[avail_fields]
            df_topics[missing_fields] = np.nan
        # Flattening list of JSONs, maintaining index from explode to assist concat()
        df_topics = df_topics.explode('topics')
        new_index = df_topics.index
        df_dict_flat = pd.json_normalize(df_topics['topics'], sep='_').set_index(new_index)
        df_dict_flat.columns = ['topic_'+col for col in df_dict_flat.columns]
        # Concat flattened records DF with main DF. The "new_index" will concat with the correct "id" records
        df_topics = pd.concat([df_topics.drop(['topics'], axis=1), df_dict_flat], axis=1, join='outer')
        df_topics.dropna(subset=['topic_id'], inplace = True)
        # Many topics to One Id relation, adding seq_no to topics as per relevance "score"
        df_topics['seq_no'] = df_topics.groupby('id')['topic_score'].rank('first', ascending = False).astype('int64')
        df_topics['pipeline_date'] = datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df_topics.rename({'id':'work_id'}, axis=1, inplace=True)
        df_topics.drop_duplicates(subset=['work_id', 'topic_id'], keep='first', inplace=True)
        self.table_df = df_topics
        self.table_json = json.loads(df_topics.to_json(orient='records'))