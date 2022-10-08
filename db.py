import os
from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import schema

@compiles(schema.CreateTable)
def compile(element, compiler, **kw):
    text = compiler.visit_create_table(element, **kw)
    if element.element.info.get('without_rowid'):
        text = text.rstrip() + ' WITHOUT ROWID\n\n'
        print("WITHOUT ROWID!!!")
    return text


def get_table_def(primary_key_datatype, **kwargs):

    metadata = MetaData()
    signals = Table('signals', metadata,
                  Column('id', primary_key_datatype, primary_key=True),
                  Column('time_created', String(64)),
                  Column('data', String(1024*10)),
                  **kwargs
                  )

    return metadata, signals


def create_db_tables(metadata, db_engine):

    try:
        metadata.create_all(db_engine)
        print("Tables created {}".format(db_engine.engine.name))
    except Exception as e:
        print("Error occurred during Table creation!")
        print(e)


def clear_db(metadata, db_engine):

    try:
        metadata.drop_all(db_engine)
        print("Tables dropped")
    except Exception as e:
        print("Error occurred during Table dropping!")
        print(e)


def pg_table_size(dburi, table):
    m = MetaData(dburi)
    e = lambda x: m.bind.execute(x).first()[0]

    query = f"SELECT pg_total_relation_size('{table}');"

    return e(query)


def mariadb_table_size(dburi, schema, table):
    m = MetaData(dburi)
    e = lambda x: m.bind.execute(x).first()[0]

    query = f'SELECT SQL_NO_CACHE (data_length + index_length) tbl_size FROM information_schema.TABLES WHERE table_schema = "{schema}" AND table_name = "{table}";'

    return e(query)


def sqlite_db_size(sqlite_file):

    return os.path.getsize(sqlite_file)
