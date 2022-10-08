import datetime
import os
import base64
from sqlalchemy import create_engine, BINARY
from sqlalchemy.dialects.postgresql import BYTEA

from db import create_db_tables, get_table_def, clear_db, mariadb_table_size, pg_table_size


def random_string(n):
    s = base64.b64encode(os.urandom(n))
    return s[:n]  # some characters are wasted


class Writer:

    def __init__(self, key_gen):
        self._key_gen = key_gen

    def generate_batch(self, batch_size):

        return [
            {
                "id": self._key_gen.get_next(),
                "time_created": datetime.datetime.now().isoformat(),
                "data": random_string(512)  # 1KB
            }
            for _ in range(batch_size)
        ]

    def init_db(self):
        """
        Clear and initialize DB table before inserting
        """
        raise NotImplementedError()

    def write_batch(self, batch_size):
        """
        Generate next batch of data and write to DB
        :param batch_size: How many records to insert
        """
        raise NotImplementedError()

    def db_size(self):
        """
        :return: Size fo database in bytes
        """

        raise NotImplementedError()

    def close(self):
        return


class SQLiteWriter(Writer):

    def __init__(self, key_gen, sqlite_file, clustered_index=True):
        super(SQLiteWriter, self).__init__(key_gen)
        self._sqlite_file = sqlite_file
        self._connection = None

        table_def_kwargs = {}
        if clustered_index:
            table_def_kwargs["info"] = {'without_rowid': True}

        self._metadata, self._sig_tbl = get_table_def(self._key_gen.datatype(), **table_def_kwargs)

    @classmethod
    def _get_engine(cls, sqlite_file):
        eng = create_engine("sqlite:///{}".format(sqlite_file))
        conn = eng.connect()
        return conn

    def _get_connection(self):
        engine = self._get_engine(self._sqlite_file)
        return engine.connect()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._get_connection()
            # self._connection.execute("PRAGMA journal_mode=WAL;")

            self._connection.execute("PRAGMA cache_size = -10240;")  # 10MB

        return self._connection

    def init_db(self):

        bind = self.connection

        clear_db(self._metadata, bind)

        create_db_tables(self._metadata, bind)

        self.connection.close()
        self._connection = None

        self.connection.execute("PRAGMA journal_mode=OFF;")  # changing page size doesn't work when WL is enabled

        bind.execute("PRAGMA page_size=16384;")  # 1 page = 16KB
        bind.execute("VACUUM")

        self.connection.execute("PRAGMA journal_mode=WAL;")

    def write_batch(self, batch_size):

        # print(self.connection.execute("SELECT * FROM dbstat WHERE name = 'signals' AND aggregate = TRUE").mappings().all())
        #
        # print(self.connection.execute("PRAGMA page_size").mappings().all())

        batch = self.generate_batch(batch_size)
        tx = self.connection.begin()

        self.connection.execute(self._sig_tbl.insert().values(batch))
        tx.commit()

    def db_size(self):
        stat = self.connection.execute("SELECT * FROM dbstat WHERE name = 'signals' AND aggregate = TRUE").mappings().all()[0]

        return stat["pgsize"]  # using aggregate mode, sot the size of table is the size of pages summed

        # return sqlite_db_size(self._sqlite_file)

    def close(self):
        self.connection.close()
        os.unlink(self._sqlite_file)


class MariaDBWriter(Writer):

    def __init__(self, key_gen, connection_string):
        super(MariaDBWriter, self).__init__(key_gen)
        self._connection_string = connection_string
        self._connection = None
        self._metadata, self._sig_tbl = get_table_def(self._key_gen.datatype())

    @classmethod
    def _get_engine(cls, connection_string):
        eng = create_engine(connection_string)
        conn = eng.connect()
        return conn

    def _get_connection(self):
        engine = self._get_engine(self._connection_string)
        return engine.connect()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._get_connection()
            # self._connection.execute("PRAGMA journal_mode=WAL;")
            #
            # self._connection.execute("PRAGMA page_size = 16384;")  # 1 page = 16KB
            #
            # self._connection.execute("PRAGMA cache_size = -640;")  # 10MB

        return self._connection

    def init_db(self):

        connection = self.connection

        clear_db(self._metadata, connection)

        create_db_tables(self._metadata, connection)

        connection.execute("SET GLOBAL innodb_buffer_pool_size=10485760;")  # for some reason configuring this in config files doesn't work

    def write_batch(self, batch_size):

        print(self.connection.execute("SHOW VARIABLES LIKE '%innodb_buffer_pool_size%';").mappings().all())

        batch = self.generate_batch(batch_size)
        tx = self.connection.begin()

        self.connection.execute(self._sig_tbl.insert().values(batch))
        tx.commit()

    def db_size(self):
        return mariadb_table_size(self._connection_string, "signals", "signals")

    def close(self):
        self.connection.close()


class PostgresWriter(Writer):

    def __init__(self, key_gen, connection_string):
        super(PostgresWriter, self).__init__(key_gen)
        self._connection_string = connection_string
        self._connection = None

        datatype = self._key_gen.datatype()

        if isinstance(datatype, BINARY):  # postgres doesnt support binary
            datatype = BYTEA(datatype.length)

        self._metadata, self._sig_tbl = get_table_def(datatype)

    @classmethod
    def _get_engine(cls, connection_string):
        eng = create_engine(connection_string)
        conn = eng.connect()
        return conn

    def _get_connection(self):
        engine = self._get_engine(self._connection_string)
        return engine.connect()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._get_connection()

            print(list(self._connection.execute("SELECT current_setting('block_size');")))
            print(list(self._connection.execute("SELECT current_setting('shared_buffers');")))

        return self._connection

    def init_db(self):

        connection = self.connection

        clear_db(self._metadata, connection)

        create_db_tables(self._metadata, connection)

    def write_batch(self, batch_size):

        batch = self.generate_batch(batch_size)
        tx = self.connection.begin()

        self.connection.execute(self._sig_tbl.insert().values(batch))
        tx.commit()

    def db_size(self):
        return pg_table_size(self._connection_string, "signals")

    def close(self):
        self.connection.close()
