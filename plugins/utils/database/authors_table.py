from datetime import datetime
import json

import numpy as np
import pandas as pd
import pytz
from utils.database.schemas import authors_schema
from utils.database.table_model import tableModel


class authorsTable(tableModel):
    '''Authors database table object'''

    def __init__(self, table_json_path=None, raw_json_path=None):

        super().__init__(authors_schema)
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
        req_fields = ['authorships']
        set_fields = set(req_fields)
        # Creating dataframe with required fields, if field absent then handling in exception
        try:
            df_author = pd.DataFrame(json_obj)[req_fields]
        except KeyError as e:
            err_str = str(e)
            #finding set of missing fields, and assigning NaN values for them
            list_end = err_str.find(']')
            missing_fields = err_str[2:list_end].replace("'", '').split(",")
            missing_set = set(missing_fields)
            avail_fields = list(set_fields.difference(missing_set))
            df_author = pd.DataFrame(json_obj)[avail_fields]
            df_author[missing_fields] = np.nan
        # Identify columns with any list type records and flatten. Done for 2 levels (list within list of JSON)
        list_bool = df_author.apply(lambda x: x.apply(lambda x: isinstance(x, list)).any())
        for col in list(list_bool[list_bool].index):
            df_author = self._flat_list_col(df_author, col)    
        list_bool = df_author.apply(lambda x: x.apply(lambda x: isinstance(x, list)).any())
        for col in list(list_bool[list_bool].index):
            df_author = self._flat_list_col(df_author, col)
        df_author = df_author[[
            'authorships_author_id', 'authorships_author_display_name', 'authorships_institutions_id', 
            'authorships_institutions_display_name', 'authorships_institutions_country_code',
            'authorships_countries']]\
            .dropna(how='all')\
            .rename({
                'authorships_author_id':'author_id', 'authorships_author_display_name':'name',
                'authorships_institutions_id':'institution_id', 
                'authorships_institutions_display_name':'institution_display_name', 
                'authorships_institutions_country_code':'institution_country_code',
                'authorships_countries':'country'}, axis=1)
        df_author['pipeline_date'] = datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
        df_author.drop_duplicates(subset=['author_id'], keep='first', inplace=True)
        self.table_df = df_author
        self.table_json = json.loads(df_author.to_json(orient='records'))


    def _flat_list_col(self, df, col):
        '''Flattens column having list type records and returns dataframe joined with flattened data'''
        
        # Exploding list to multiple rows
        df_exp = df[[col]].explode(column = col, ignore_index = False)
        df_exp = df_exp.reset_index(drop=False).groupby(['index'])\
            .agg({col:'first'})
        # If JSON type records, then flattened, else directly joined with main dataframe
        if df_exp[col].apply(lambda x: isinstance(x, dict)).any():        
            new_index = df_exp.index
            df_norm = pd.json_normalize(df_exp[col], sep = "_")
            df_norm = df_norm.set_index(new_index).reset_index(drop=False)
            columns = (df_norm.columns)[1:]
            agg_dict= {}
            for i in columns:
                agg_dict[i] = 'first'
            df_norm = df_norm.groupby(['index']).agg(agg_dict)
            df_norm.columns = [col+"_"+ele for ele in df_norm.columns]
            df_concat = pd.concat([df.drop([col], axis = 1), df_norm], axis = 1).fillna(np.nan)
        else:
            df_norm = df_exp
            df_concat = pd.concat([df.drop([col], axis = 1), df_norm], axis = 1).fillna(np.nan)
        return df_concat

    