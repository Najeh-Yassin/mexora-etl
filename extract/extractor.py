import pandas as pd
import json
from config.settings import FILES
from utils.logger import setup_logger

logger = setup_logger()

def extract_data():
    """
    Lit les fichiers CSV et JSON sources.
    """
    logger.info("Début de l'extraction des données...")
    
    try:
        # Extraction Commandes
        df_commandes = pd.read_csv(FILES["commandes"])
        # Gestion des formats de date mixtes (ex: 2023-01-01 et Jun 02 2022)
        df_commandes["date_commande"] = pd.to_datetime(df_commandes["date_commande"], format='mixed')
        logger.info(f"Commandes extraites : {len(df_commandes)} lignes.")
        
        # Extraction Clients
        df_clients = pd.read_csv(FILES["clients"])
        logger.info(f"Clients extraits : {len(df_clients)} lignes.")
        
        # Extraction Produits (JSON)
        with open(FILES["produits"], 'r', encoding='utf-8') as f:
            data_json = json.load(f)
        df_produits = pd.DataFrame(data_json["produits"])
        logger.info(f"Produits extraits : {len(df_produits)} lignes.")
        
        # Extraction Régions
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