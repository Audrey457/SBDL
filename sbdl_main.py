import sys

from lib import Utils, DataLoader
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    #Créer session spark
    spark = Utils.get_spark_session(job_run_env)

    #Créer le logger
    logger = Log4j(spark)

    #Lire la df account et la transformer
    df_account = DataLoader.read_accounts(spark, job_run_env,False,"" )
    df_account.show()

    #Lire la df parties et la transformer
    df_parties = DataLoader.read_parties(spark, job_run_env, False, "")
    df_parties.show()

    #Lire les adresses et les transformer
    df_addresses = DataLoader.read_address(spark, job_run_env, False, "")
    df_addresses.show()

    #Joindre les parties et les adresses

    #Joindre les accounts aux parties_adress

    #Ajouter le header event => on obtient le final_df

    #Pour envoyer une dataframe à Kafka, il faut que ce soit une df à 2 colonnes
    #La première est une clé, la seconde une valeur
    #Clé = contractIdentifier.newValue
    #Valeur = packager tout (*) sous forme de structure
    #ça doit être sous format json (to_json)

    logger.info("Finished creating Spark Session")
