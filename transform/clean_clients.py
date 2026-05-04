import pandas as pd
import re
import logging
from datetime import date

logger = logging.getLogger(__name__)

def clean_clients(df, df_commandes=None):
    """
    Nettoyage des clients (R1 à R4).
    La segmentation client (R5) est déplacée dans build_dim_client pour éviter redondance.
    """
    logger.info("Nettoyage des clients...")
    df = df.copy()
    initial = len(df)

    # R1 — Déduplication sur email normalisé
    df['email_norm'] = df['email'].str.lower().str.strip()
    avant = len(df)
    df = df.sort_values('date_inscription', ascending=False).drop_duplicates(subset=['email_norm'], keep='first')
    logger.info(f"R1 déduplication email : {avant - len(df)} doublons supprimés")
    df.drop(columns=['email_norm'], inplace=True)

    # R2 — Standardisation du sexe
    mapping_sexe = {
        'm': 'm', 'f': 'f', '1': 'm', '0': 'f',
        'homme': 'm', 'femme': 'f', 'male': 'm', 'female': 'f', 'h': 'm'
    }
    if 'sexe' in df.columns:
        avant_sexe = df['sexe'].nunique()
        df['sexe'] = df['sexe'].astype(str).str.lower().str.strip().map(mapping_sexe).fillna('inconnu')
        logger.info(f"R2 sexe : {avant_sexe} valeurs d'origine → standardisé en m/f/inconnu")

    # R3 — Validation des dates de naissance (âge entre 16 et 100 ans)
    df['date_naissance'] = pd.to_datetime(df['date_naissance'], errors='coerce')
    today = pd.Timestamp(date.today())
    df['age'] = (today - df['date_naissance']).dt.days // 365
    ages_invalides = ((df['age'] < 16) | (df['age'] > 100)).sum()
    df.loc[(df['age'] < 16) | (df['age'] > 100), 'date_naissance'] = pd.NaT
    # Tranche d'âge
    df['tranche_age'] = pd.cut(df['age'].fillna(0),
                               bins=[0, 18, 25, 35, 45, 55, 65, 200],
                               labels=['<18', '18-24', '25-34', '35-44', '45-54', '55-64', '65+'])
    logger.info(f"R3 âge : {ages_invalides} lignes avec âge invalide (mis à NaT)")

    # R4 — Validation email (format)
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    emails_invalides = ~df['email'].str.match(pattern, na=False)
    df.loc[emails_invalides, 'email'] = None
    logger.info(f"R4 email : {emails_invalides.sum()} emails invalides mis à NULL")

    logger.info(f"Clients après nettoyage (sans segmentation) : {len(df)} lignes ({initial - len(df)} supprimées)")
    return df