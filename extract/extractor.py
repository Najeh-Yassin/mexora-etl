import pandas as pd
import json
from config.settings import FILES
from utils.logger import setup_logger

logger = setup_logger()

def extract_data():
    logger.info("Début de l'extraction des données...")
    try:
        # Extraction Commandes : tout en string pour éviter conversions implicites
        df_commandes = pd.read_csv(FILES["commandes"], dtype=str)
        df_commandes["date_commande"] = pd.to_datetime(df_commandes["date_commande"], format='mixed')
        logger.info(f"Commandes extraites : {len(df_commandes)} lignes.")

        # Extraction Clients
        df_clients = pd.read_csv(FILES["clients"], dtype=str)
        logger.info(f"Clients extraits : {len(df_clients)} lignes.")

        # Extraction Produits (JSON)
        with open(FILES["produits"], 'r', encoding='utf-8') as f:
            data_json = json.load(f)
        df_produits = pd.DataFrame(data_json["produits"])
        logger.info(f"Produits extraits : {len(df_produits)} lignes.")

        # Extraction Régions (propre)
        df_regions = pd.read_csv(FILES["regions"])
        logger.info(f"Régions extraites : {len(df_regions)} lignes.")

        return {
            "commandes": df_commandes,
            "clients": df_clients,
            "produits": df_produits,
            "regions": df_regions
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction : {e}")
        raise