import pandas as pd
from utils.logger import setup_logger

logger = setup_logger()

def clean_produits(df):
    """
    Nettoyage des données produits.
    """
    logger.info("Nettoyage des produits...")
    
    # 1. Normalisation casse catégories
    if "categorie" in df.columns:
        df["categorie"] = df["categorie"].str.strip().str.capitalize()
    
    # 2. Gestion produits inactifs (exemple: filtrer ou marquer)
    # Ici, on s'assure juste que les colonnes essentielles existent
    df["est_actif_source"] = df.get("est_actif", True)
    
    logger.info(f"Produits après nettoyage : {len(df)} lignes.")
    return df