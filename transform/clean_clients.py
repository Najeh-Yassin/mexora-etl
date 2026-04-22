import pandas as pd
import re
import logging
from datetime import date
from utils.logger import setup_logger

logger = setup_logger()

def validate_email(email):
    """Simple regex pour valider l'email."""
    if pd.isna(email): return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email)))

def clean_clients(df, df_commandes):
    """
    Nettoyage complet des clients (R1-R5).
    """
    logger.info("Nettoyage des clients et calcul des segments...")
    df = df.copy()
    
    # R1 & R4 — Email (Validation format + Déduplication)
    df = df[df["email"].apply(validate_email)]
    df = df.drop_duplicates(subset=["email"], keep='last')
    
    # R2 — Normalisation du sexe (m/f/inconnu)
    if "sexe" in df.columns:
        df["sexe"] = df["sexe"].str.lower().map({
            "homme": "m", "femme": "f", "m": "m", "f": "f", "1": "m", "0": "f"
        }).fillna("inconnu")

    # R3 — Validation des dates de naissance (16 - 100 ans)
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce')
    today = date.today()
    # Calcul de l'âge approximatif
    df['age'] = df['date_naissance'].apply(lambda x: today.year - x.year if pd.notnull(x) else None)
    # On filtre : on ne garde que les gens entre 16 et 100 ans (ou NaN si on veut être souple)
    df = df[(df['age'].isna()) | ((df['age'] >= 16) & (df['age'] <= 100))]
    
    # R5 — CALCUL DE LA SEGMENTATION
    # Calcul du CA total par client
    df_cmd_valides = df_commandes.copy()
    df_cmd_valides['ca'] = df_cmd_valides['quantite'].astype(float) * df_cmd_valides['prix_unitaire'].astype(float)
    
    # Segmentation basée sur le CA total (uniquement livrés)
    ca_par_client = df_cmd_valides[df_cmd_valides['statut'].str.contains('livré|done', case=False, na=False)] \
                    .groupby('id_client')['ca'].sum().reset_index()
    
    def definir_segment(ca):
        if ca >= 15000: return 'Gold'
        if ca >= 5000: return 'Silver'
        return 'Bronze'
    
    ca_par_client['segment_client'] = ca_par_client['ca'].apply(definir_segment)
    
    # Fusion avec la table client
    df = df.merge(ca_par_client[['id_client', 'segment_client']], on='id_client', how='left')
    df['segment_client'] = df['segment_client'].fillna('Bronze')

    logger.info(f"Clients après nettoyage et segmentation : {len(df)} lignes.")
    return df