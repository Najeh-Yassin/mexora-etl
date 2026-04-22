import pandas as pd
import logging

def clean_commandes(df):
    logging.info("Nettoyage des commandes...")
    
    # On crée une copie propre pour éviter les erreurs de manipulation
    df = df.copy()

    # R1 — Suppression des doublons
    df = df.drop_duplicates(subset=['id_commande'], keep='last')

    # R2 — Standardisation des dates
    df["date_commande"] = pd.to_datetime(df["date_commande"], errors='coerce')
    df = df.dropna(subset=["date_commande"])

    # --- LA CORRECTION EST ICI ---
    # On ne force PAS 'int'. On garde le texte (ex: LIV_094)
    # On remplace les vides par 'INCONNU'
    df["id_livreur"] = df["id_livreur"].fillna("INCONNU").astype(str)
    # -----------------------------

    # Conversion propre des prix et quantités en nombres (si possible)
    df["quantite"] = pd.to_numeric(df["quantite"], errors='coerce').fillna(0)
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors='coerce').fillna(0)

    # R5 & R6 — Suppression des erreurs de saisie
    df = df[df["quantite"] > 0]
    df = df[df["prix_unitaire"] > 0]

    logging.info(f"Nettoyage terminé. Lignes valides : {len(df)}")
    return df