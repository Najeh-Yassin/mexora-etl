import pandas as pd
import logging

def clean_commandes(df):
    logging.info("Nettoyage des commandes...")
    df = df.copy()

    # R1 — Suppression des doublons
    df = df.drop_duplicates(subset=['id_commande'], keep='last')

    # R2 — Standardisation des dates
    df["date_commande"] = pd.to_datetime(df["date_commande"], errors='coerce')
    df = df.dropna(subset=["date_commande"])

    # R3 — Harmonisation des noms de villes (Problèmes intentionnels : TNG, Tnja, tanger)
    mapping_villes = {
        'tng': 'Tanger', 'tnja': 'Tanger', 'tanger': 'Tanger', 'tng ': 'Tanger',
        'casa': 'Casablanca', 'casablanca': 'Casablanca', 'kech': 'Marrakech',
        'rabat': 'Rabat', 'fes': 'Fès', 'oujda': 'Oujda', 'agadir': 'Agadir'
    }
    # On nettoie la colonne ville_livraison
    df['ville_livraison'] = df['ville_livraison'].str.strip().str.lower().replace(mapping_villes)
    # On met la première lettre en majuscule pour le reste
    df['ville_livraison'] = df['ville_livraison'].str.title()

    # R7 — Gestion id_livreur
    df["id_livreur"] = df["id_livreur"].fillna("INCONNU").astype(str)

    # R5 & R6 — Quantités et prix (Conversion et filtre)
    df["quantite"] = pd.to_numeric(df["quantite"], errors='coerce').fillna(0)
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors='coerce').fillna(0)
    df = df[(df["quantite"] > 0) & (df["prix_unitaire"] > 0)]

    # Standardisation des statuts pour éviter les erreurs SQL
    df['statut'] = df['statut'].str.lower().str.strip().replace({
        'ok': 'en_cours', 'done': 'livré', 'livre': 'livré', 'ko': 'annulé'
    })

    logging.info(f"Nettoyage terminé. Lignes valides : {len(df)}")
    return df