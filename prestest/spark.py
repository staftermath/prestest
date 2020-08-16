import pytest
import os

SPARK_CONFIGURATION = {
    "spark.submit.deployMode": "client",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": 1,
    "spark.hive.metastore.uris": "thrift://localhost:9083",
    "spark.hadoop.dfs.namenode.http-address": "webhdfs://localhost:50070"
}


@pytest.fixture(scope="session")
def spark(tmpdir_factory):
    from pyspark.sql import SparkSession
    from pyspark import SparkConf
    spark_conf_dir = tmpdir_factory.mktemp("test_spark_conf_dir")
    spark_warehouse = tmpdir_factory.mktemp("test_spark_warehouse")
    spark_metastore_db = tmpdir_factory.mktemp("test_metastore_db")
    os.environ["SPARK_CONF_DIR"] = str(spark_conf_dir)
    spark_conf_str = f"spark.sql.warehouse.dir {spark_warehouse}\n" + \
                     f"spark.driver.extraJavaOptions -Dderby.system.home={spark_metastore_db}"
    conf_file = spark_conf_dir.join("spark-defaults.conf")
    with open(conf_file, 'w') as f:
        f.write(spark_conf_str)
    spark_conf = SparkConf()
    for attribute, value in SPARK_CONFIGURATION.items():
        spark_conf.set(attribute, value)

    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    return spark
