import pytest
from lib.Utils import get_spark_session
from pyspark.sql.functions import *

@pytest.fixture
def spark():
    "creates spark session"
    spark =  get_spark_session("LOCAL")
    yield spark
    spark.stop()

@pytest.fixture
def expected_results(spark):
    "gives the expected results"
    results_schema="state string, count int"
    return spark.read \
    .format("csv") \
    .schema(results_schema) \
    .load("data/test_results/state_aggregate.csv") 