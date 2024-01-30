import sys

from lib import Utils, DataLoader, ConfigLoader
from lib.Transformations import *
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    #Récupérer informations de configuration
    conf = ConfigLoader.get_config(job_run_env)
    enable_hive = True if conf["enable.hive"] == "true" else False
    hive_db = conf["hive.database"]

    #Créer session spark
    spark = Utils.get_spark_session(job_run_env)

    #Créer le logger
    logger = Log4j(spark)

    logger.info("Finished creating Spark Session")

    #Lire la df account et la transformer
    df_account = DataLoader.read_accounts(spark, job_run_env,enable_hive,hive_db )
    df_account_tf = get_contract(df_account)

    #Lire la df parties et la transformer
    df_parties = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    df_parties_tf = get_relations(df_parties)

    #Lire les adresses et les transformer
    df_addresses = DataLoader.read_address(spark, job_run_env, enable_hive, hive_db)
    df_addresses_tf = get_address(df_addresses)

    #Joindre les parties et les adresses
    df_parties_addresses = join_party_adress(df_parties_tf, df_addresses_tf)

    #Joindre les accounts aux parties_adress
    df_account_parties_addresses = join_contract_party(df_account_tf, df_parties_addresses)

    #Ajouter le header event => on obtient le final_df
    final_df = apply_header(spark, df_account_parties_addresses)

    #Pour envoyer une dataframe à Kafka, il faut que ce soit une df à 2 colonnes
    #La première est une clé, la seconde une valeur
    #Clé = contractIdentifier.newValue
    #Valeur = packager tout (*) sous forme de structure
    #ça doit être sous format json (to_json)
    kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"),
                                  to_json(struct("*")).alias("value"))
    kafka_kv_df.show()
