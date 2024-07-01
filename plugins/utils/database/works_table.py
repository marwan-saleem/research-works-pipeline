from datetime import datetime
import json

import numpy as np
import pandas as pd
import pytz
from utils.database.schemas import works_schema
from utils.database.table_model import tableModel


class worksTable(tableModel):
    '''Works database table object'''

    def __init__(self, table_json_path=None, raw_json_path=None):

        super().__init__(works_schema)
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
        '''Transformation logic of raw JSON to DF'''

        json_obj = self._read_json_file(self.raw_json_path)
        req_fields = ['id', 'doi', 'display_name', 'relevance_score',
            'authorships', 'publication_date', 'primary_location',
            'referenced_works_count', 'cited_by_count',
            'updated_date', 'created_date']
        set_fields = set(req_fields)
        # Creating dataframe with required fields, if field absent then handling in exception
        try:
            df_works = pd.DataFrame(json_obj)[req_fields]
        except KeyError as e:
            err_str = str(e)
            # finding set of missing fields, and assigning NaN values for them
            list_end = err_str.find(']')
            missing_fields = err_str[2:list_end].replace("'", '').split(",")
            missing_set = set(missing_fields)
            avail_fields = list(set_fields.difference(missing_set))
            df_works = pd.DataFrame(json_obj)[avail_fields]
            df_works[missing_fields] = np.nan
        # Flatten columns with lists
        list_bool = df_works.apply(lambda x: x.apply(lambda x: isinstance(x, list)).any())
        for col in list(list_bool[list_bool].index):
            df_works = self._flat_list_col(df_works, col)
        # Flatten columns with dictionaries
        dict_bool = df_works.apply(lambda x: x.apply(lambda y: isinstance(y, dict)).any( ))
        for col in list(dict_bool[dict_bool].index):
            df_works = self._flat_dict_col(df_works, col)
        df_works = df_works[[
            'id', 'doi', 'display_name', 'authorships_author_id',
            'primary_location_source_id', 'primary_location_source_is_oa',
            'relevance_score', 'referenced_works_count', 'cited_by_count',
            'publication_date', 'created_date', 'updated_date']]\
            .rename({
                'id':'work_id',
                'authorships_author_id':'author_id', 'primary_location_source_id':'source_id',
                'primary_location_source_is_oa':'is_open_source'}, axis=1)
        df_works['pipeline_date'] = datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df_works.drop_duplicates(subset=['work_id'], keep='first', inplace=True)
        self.table_df = df_works
        self.table_json = json.loads(df_works.to_json(orient='records'))        
    
    def _flat_list_col(self, df, col):
        '''Flatten column with list of JSONs type records and join flat data with main DF'''

        df_exp = df[[col]].explode(column = col, ignore_index = False)
        df_exp = df_exp.reset_index(drop=False).groupby(['index'])\
            .agg({col:'first'})
        new_index = df_exp.index
        df_norm = pd.json_normalize(df_exp[col], sep = "_")
        df_norm = df_norm.set_index(new_index).reset_index(drop=False)
        columns = (df_norm.columns)[1:]
        # keeping first record for when 1 id has multiple flattened records
        agg_dict= {}
        for i in columns:
            agg_dict[i] = 'first'
        df_norm = df_norm.groupby(['index']).agg(agg_dict)
        df_norm.columns = [col+"_"+ele for ele in df_norm.columns]
        df_concat = pd.concat([df.drop([col], axis = 1), df_norm], axis = 1).fillna(np.nan)
        return df_concat

    def _flat_dict_col(self, df, col):
        '''Flatten column with JSON type records and join flat data with main DF'''
        
        df_norm = pd.json_normalize(df[col], sep = '_')
        df_norm.columns = [col+"_"+ele for ele in df_norm.columns]
        df_concat = pd.concat([df.drop([col], axis=1), df_norm], axis = 1)
        return df_concat

    