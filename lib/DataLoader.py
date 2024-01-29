import configparser
from pyspark import SparkConf
from pyspark.sql.types import *

from lib.ConfigLoader import get_data_filter


#permet de définir le schema du fichier csv
def get_account_schema():
    return StructType([
        StructField("load_date", DateType()),
        StructField("active_ind", IntegerType()),
        StructField("account_id", StringType()),
        StructField("source_sys", StringType()),
        StructField("account_start_date", TimestampType()),
        StructField("legal_title_1", StringType()),
        StructField("legal_title_2", StringType()),
        StructField("tax_id_type", StringType()),
        StructField("tax_id", StringType()),
        StructField("branch_code", StringType()),
        StructField("country", StringType())
    ])
def get_party_schema():
    schema = """load_date date,account_id string,party_id string,
    relation_type string,relation_start_date timestamp"""
    return schema


def get_address_schema():
    schema = """load_date date,party_id string,address_line_1 string,
    address_line_2 string,city string,postal_code string,
    country_of_address string,address_start_date date"""
    return schema

#enable_hive et hive_db c'est pour env prod et qa, pour local on lit le csv.
#on devra donc récupérer dans un premier temps le data_filter pour le where, puis
#   - si enable_hive : lire la table <hive_db>.accounts
#   - sinon : lire le csv
def read_accounts(spark, env, enable_hive, hive_db):
    filter = get_data_filter(env, "account.filter")
    schema = get_account_schema()
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".accounts").where(filter)
    return spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .option("mode", "PERMISSIVE") \
        .load("test_data/accounts/account_samples.csv") \
        .where(filter)

def read_parties(spark, env, enable_hive, hive_db):
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".parties")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_party_schema()) \
            .load("test_data/parties/party_samples.csv")


def read_address(spark, env, enable_hive, hive_db):
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".party_address")
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_address_schema()) \
            .load("test_data/party_address/address_samples.csv")