import os
import time

import pytest
from pyspark.sql import SparkSession

# Python converts collected timestamps with the local timezone: pin everything to UTC
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("telecare-tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


class MemorySink:
    """Records what would be written; lets tests assert on side effects."""

    def __init__(self):
        self.merged: dict[str, list] = {}
        self.appended: dict[str, list] = {}

    def merge_insert_only(self, df, table, keys):
        self.merged.setdefault(table, []).extend(r.asDict() for r in df.collect())

    def append(self, df, table, batch_id, app_id):
        self.appended.setdefault(table, []).extend(r.asDict() for r in df.collect())


@pytest.fixture
def sink():
    return MemorySink()
