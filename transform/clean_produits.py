import pandas as pd
import logging

def transform_produits(df):
    initial = len(df)
    
    # R1 - Harmonisation de la casse des catégories
    # "electronique", "Electronique", "ELECTRONIQUE" -> "Electronique"
    if 'categorie' in df.columns:
        df['categorie'] = df['categorie'].str.strip().str.capitalize()
    
    # R2 - Gestion des prix catalogue à null
    # On remplace par 0 ou on supprime selon la stratégie. Ici on supprime les produits sans prix.
    df['prix_catalogue'] = pd.to_numeric(df['prix_catalogue'], errors='coerce')
    df = df.dropna(subset=['prix_catalogue'])
    
    # R3 - Nettoyage des libellés (espaces superflus)
    df['nom'] = df['nom'].str.strip()
    df['marque'] = df['marque'].str.strip()

    logging.info(f"[TRANSFORM] Produits : {initial} -> {len(df)} lignes.")
    return df