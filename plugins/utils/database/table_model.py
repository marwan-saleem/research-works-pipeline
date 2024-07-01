import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert


class tableModel():
    ''' Object that represents database tables in dataframes '''

    def __init__(self, table_schema):

        self.table_schema = table_schema
        self.table_json = None
        self.table_df = None   

    def write_table_json(self, folder_path):

        json_str = json.dumps(self.table_json)
        filename = f"\\{self.table_schema.name}_table_json.json"
        filepath = folder_path+filename
        with open(filepath, 'w') as file:
            file.write(json_str)

    def read_table_json(self, folder_path):

        filename = f"\\{self.table_schema.name}_table_json.json"
        filepath = folder_path+filename
        self.table_json = self._read_json_file(filepath)

    def upsert_statement(self):
        '''Creates and returns upsert statement'''

        stmt = insert(self.table_schema).values(self.table_json)
        pk_cols = list(map(lambda x: x.name, list(self.table_schema.primary_key)))
        stmt = stmt.on_conflict_do_update(
            index_elements=pk_cols,
            set_=dict(stmt.excluded)
        )
        return stmt

    def _read_json_file(self, file_path):
        
        with open(file_path, 'r') as file:
            json_str = file.read()
        json_obj = json.loads(json_str)
        return json_obj

    