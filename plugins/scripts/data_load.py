import json

from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import insert
from utils.database.works_table import worksTable
from utils.database.authors_table import authorsTable
from utils.database.sources_table import sourcesTable
from utils.database.topics_table import topicsTable
from utils.database.keywords_table import keywordsTable


def data_load(DB_NAME, DB_USER, DB_PASS, DB_HOST, 
              DB_PORT, archive_folder):
    '''Takes database creds and archive path, and loads all tables in database'''
    
    works_obj = worksTable(table_json_path=archive_folder)
    authors_obj = authorsTable(table_json_path=archive_folder)
    sources_obj = sourcesTable(table_json_path=archive_folder)
    topics_obj = topicsTable(table_json_path=archive_folder)
    keywords_obj = keywordsTable(table_json_path=archive_folder)
    # writing df records to DB 
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    with engine.begin() as conn:
        conn.execute(works_obj.upsert_statement())
        conn.execute(authors_obj.upsert_statement())
        conn.execute(sources_obj.upsert_statement())
        conn.execute(topics_obj.upsert_statement())
        conn.execute(keywords_obj.upsert_statement())


