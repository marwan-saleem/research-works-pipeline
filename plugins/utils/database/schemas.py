from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Date, DateTime, MetaData


metadata = MetaData()
works_schema = Table('works', metadata,
                    Column("work_id", String, primary_key=True), Column("doi", String), Column("display_name", String), 
                    Column("author_id", String), Column("source_id", String), 
                    Column("is_open_source", String), 
                    Column("relevance_score", Float), Column("referenced_works_count", Integer), 
                    Column("cited_by_count", Integer), 
                    Column("publication_date", Date), Column("created_date", Date), 
                    Column("updated_date", DateTime), Column("pipeline_date", DateTime))
authors_schema = Table('authors', metadata,
                      Column("author_id", String, primary_key=True), Column("name", String), Column("institution_id", String), 
                      Column("institution_display_name", String), Column("institution_country_code", String), 
                      Column("country", String), 
                      Column("pipeline_date", DateTime))
sources_schema = Table('sources', metadata,
                     Column("source_id", String, primary_key=True), Column("source_display_name", String), 
                     Column("is_oa", String), Column("pipeline_date", DateTime))
topics_schema = Table('topics', metadata,
                     Column("work_id", String, primary_key=True), Column("topic_id", String, primary_key=True), 
                     Column("topic_display_name", String), 
                     Column("topic_score", Float), Column("topic_subfield_id", String), 
                     Column("topic_subfield_display_name", String), 
                     Column("topic_field_id", String), Column("topic_field_display_name", String), 
                     Column("topic_domain_id", String), 
                     Column("topic_domain_display_name", String), Column("seq_no", Integer), 
                     Column("pipeline_date", DateTime))
keywords_schema = Table('keywords', metadata,
                     Column("work_id", String, primary_key=True), Column("keyword_id", String, primary_key=True), 
                     Column("keyword_display_name", String), 
                     Column("keyword_score", Float), Column("seq_no", Integer), Column("pipeline_date", DateTime))                  