import pandas as pd
import re
from utils.logger import setup_logger

logger = setup_logger()

def validate_email(email):
    """Simple regex pour valider l'email."""
    if pd.isna(email): return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email)))

def clean_clients(df):
    """
    Nettoyage des données clients.
    """
    logger.info("Nettoyage des clients...")
    
    # 1. Validation du format email
    df = df[df["email"].apply(validate_email)]
    
    # 2. Déduplication par email
    df = df.drop_duplicates(subset=["email"])
    
    # 3. Normalisation du sexe (m/f/inconnu)
    if "sexe" in df.columns:
        df["sexe"] = df["sexe"].str.lower().map({"homme": "m", "femme": "f", "m": "m", "f": "f"}).fillna("inconnu")
    else:
        df["sexe"] = "inconnu"
        
    logger.info(f"Clients après nettoyage : {len(df)} lignes.")
    return df