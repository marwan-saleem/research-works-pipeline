import json
import os
import sys

import numpy as np
import pandas as pd
from utils.database.works_table import worksTable
from utils.database.authors_table import authorsTable
from utils.database.sources_table import sourcesTable
from utils.database.topics_table import topicsTable
from utils.database.keywords_table import keywordsTable


def works_transform(raw_json_path, archive_folder):

    works_obj = worksTable(raw_json_path = raw_json_path)
    works_obj.write_table_json(archive_folder)

def authors_transform(raw_json_path, archive_folder):

    authors_obj = authorsTable(raw_json_path=raw_json_path)
    authors_obj.write_table_json(archive_folder)

def sources_transform(raw_json_path, archive_folder):

    sources_obj = sourcesTable(raw_json_path=raw_json_path)
    sources_obj.write_table_json(archive_folder)

def topics_transform(raw_json_path, archive_folder):

    topics_obj = topicsTable(raw_json_path=raw_json_path)
    topics_obj.write_table_json(archive_folder)

def keywords_transform(raw_json_path, archive_folder):
    
    keywords_obj = keywordsTable(raw_json_path=raw_json_path)
    keywords_obj.write_table_json(archive_folder)