# import pyjion; pyjion.enable()
import json
import time

from docker_utils import get_container_io
from key_gen import RandomInt64KeyGenerator, SequentialInt64KeyGenerator, UUID1KeyGenerator,\
    UUID1FastRolloverKeyGenerator, UUID4KeyGenerator, UUID6KeyGenerator, UUID7KeyGenerator
from writer import PostgresWriter, SQLiteWriter, MariaDBWriter
import uuid
import datetime
import random

random.seed(42)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def key_gen_uuidc():
    return str(datetime.datetime.now(datetime.timezone.utc).timestamp())

    return "%08x.%s" % (int(datetime.datetime.now(datetime.timezone.utc).timestamp()), str(uuid.uuid4()))


MARIADB_CONN = "mariadb+mariadbconnector://root:pass@primary_key_perf_mariadb_1/signals"
POSTGRES_CONN = "postgresql+psycopg2://user:pass@primary_key_perf_postgres_1/signals"

SQLITE_FILE = "/tmp/sql.sqlite"

run_configs = [

    ("sqlite_clustered_int64_random", SQLiteWriter(RandomInt64KeyGenerator(), SQLITE_FILE), "primary_key_perf_benchmark_1"),
    ("sqlite_clustered_int64_sequential", SQLiteWriter(SequentialInt64KeyGenerator(), SQLITE_FILE), "primary_key_perf_benchmark_1"),

    ("sqlite_nonclustered_int64_random", SQLiteWriter(RandomInt64KeyGenerator(), SQLITE_FILE, clustered_index=False),
     "primary_key_perf_benchmark_1"),
    ("sqlite_nonclustered_int64_sequential",
     SQLiteWriter(SequentialInt64KeyGenerator(), SQLITE_FILE, clustered_index=False),
     "primary_key_perf_benchmark_1"),

    ("mariadb_random", MariaDBWriter(RandomInt64KeyGenerator(), MARIADB_CONN), "primary_key_perf_mariadb_1"),
    ("mariadb_sequential", MariaDBWriter(SequentialInt64KeyGenerator(), MARIADB_CONN), "primary_key_perf_mariadb_1"),

    ("postgres_random", PostgresWriter(RandomInt64KeyGenerator(), POSTGRES_CONN), "primary_key_perf_postgres_1"),
    ("postgres_sequential", PostgresWriter(SequentialInt64KeyGenerator(), POSTGRES_CONN), "primary_key_perf_postgres_1"),

    ("sqlite_clustered_uuid1", SQLiteWriter(UUID1KeyGenerator(), SQLITE_FILE),
     "primary_key_perf_benchmark_1"),  # SQLite is used as a library, so we're measuring I/O of the test script itself
    ("sqlite_clustered_uuid1_fast_rollover", SQLiteWriter(UUID1FastRolloverKeyGenerator(), SQLITE_FILE),
     "primary_key_perf_benchmark_1"),
    ("sqlite_clustered_uuid4", SQLiteWriter(UUID4KeyGenerator(), SQLITE_FILE),
     "primary_key_perf_benchmark_1"),
    ("sqlite_clustered_uuid6", SQLiteWriter(UUID6KeyGenerator(), SQLITE_FILE),
     "primary_key_perf_benchmark_1"),
    ("sqlite_clustered_uuid7", SQLiteWriter(UUID7KeyGenerator(), SQLITE_FILE),
     "primary_key_perf_benchmark_1"),

    ("mariadb_uuid1", MariaDBWriter(UUID1KeyGenerator(), MARIADB_CONN),
     "primary_key_perf_mariadb_1"),  # SQLite is used as a library, so we're measuring I/O of the test script itself
    ("mariadb_uuid1_fast_rollover", MariaDBWriter(UUID1FastRolloverKeyGenerator(), MARIADB_CONN),
     "primary_key_perf_mariadb_1"),
    ("mariadb_uuid4", MariaDBWriter(UUID4KeyGenerator(), MARIADB_CONN),
     "primary_key_perf_mariadb_1"),
    ("mariadb_uuid6", MariaDBWriter(UUID6KeyGenerator(), MARIADB_CONN),
     "primary_key_perf_mariadb_1"),
    ("mariadb_uuid7", MariaDBWriter(UUID7KeyGenerator(), MARIADB_CONN),
     "primary_key_perf_mariadb_1"),

    ("postgres_uuid1", PostgresWriter(UUID1KeyGenerator(), POSTGRES_CONN),
     "primary_key_perf_postgres_1"),  # SQLite is used as a library, so we're measuring I/O of the test script itself
    ("postgres_uuid1_fast_rollover", PostgresWriter(UUID1FastRolloverKeyGenerator(), POSTGRES_CONN),
     "primary_key_perf_postgres_1"),
    ("postgres_uuid4", PostgresWriter(UUID4KeyGenerator(), POSTGRES_CONN),
     "primary_key_perf_postgres_1"),
    ("postgres_uuid6", PostgresWriter(UUID6KeyGenerator(), POSTGRES_CONN),
     "primary_key_perf_postgres_1"),
    ("postgres_uuid7", PostgresWriter(UUID7KeyGenerator(), POSTGRES_CONN),
     "primary_key_perf_postgres_1"),

]


BATCH_SIZE = 1000
BATCH_COUNT = 1000


def run(run_config):
    config_name, writer, io_container = run_config

    print(f"Initializing writer for {config_name}")
    writer.init_db()

    written_records_series = []
    db_size_series = []
    db_io_read_series = []
    db_io_write_series = []

    time_series = []

    start_time = time.monotonic()
    written_records = 0

    starting_io_reads, starting_io_writes = get_container_io(io_container)

    for batch_i in range(BATCH_COUNT):
        writer.write_batch(BATCH_SIZE)
        written_records += BATCH_SIZE

        written_records_series.append(written_records)
        db_size = writer.db_size()
        db_size_series.append(db_size)
        io_read, io_write = get_container_io(io_container)
        io_read, io_write = io_read - starting_io_reads, io_write - starting_io_writes

        db_io_read_series.append(io_read)
        db_io_write_series.append(io_write)

        time_elapsed = time.monotonic() - start_time
        time_series.append(time_elapsed)

        print(f"{config_name}, written {written_records} total records")
        print(f"DB size: {sizeof_fmt(db_size)}")
        print(f"IO reads: {sizeof_fmt(io_read)} ; writes: {sizeof_fmt(io_write)}")

    writer.close()

    return {
        "name": config_name,
        "rows_written": written_records_series,
        "db_size": db_size_series,
        "io_read": db_io_read_series,
        "io_write": db_io_write_series,
        "time_elapsed": time_series
    }


for run_config in run_configs:
    print(f"Running {run_config[0]}")
    data = run(run_config)

    with open(f"results/{run_config[0]}.json", "w") as f:
        json.dump(data, f)
