import pandas as pd
import logging
from config.settings import PATH_REGIONS

logger = logging.getLogger(__name__)

def _load_ville_mapping():
    """
    Charge le référentiel régions_maroc.csv et retourne un dictionnaire
    {nom_brut_normalisé: nom_standard} pour harmoniser les villes.
    """
    df_ref = pd.read_csv(PATH_REGIONS)
    # On suppose que le fichier contient une colonne 'nom_ville_standard'
    mapping = {}
    for _, row in df_ref.iterrows():
        standard = row['nom_ville_standard'].strip()
        for variante in [standard.lower(), standard.upper(), standard.title()]:
            mapping[variante] = standard
    # Ajout manuel de quelques variantes courantes (optionnel)
    mapping.update({
        'tng': 'Tanger', 'tnja': 'Tanger', 'tanger': 'Tanger',
        'casa': 'Casablanca', 'casablanca': 'Casablanca',
        'kech': 'Marrakech', 'rabat': 'Rabat', 'fes': 'Fès',
        'oujda': 'Oujda', 'agadir': 'Agadir'
    })
    return mapping

def clean_commandes(df):
    logger.info("Nettoyage des commandes...")
    df = df.copy()
    initial = len(df)

    # R1 — Suppression des doublons
    avant = len(df)
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    logger.info(f"R1 doublons : {avant - len(df)} lignes supprimées")

    # R2 — Standardisation des dates
    df["date_commande"] = pd.to_datetime(df["date_commande"], errors='coerce', format='mixed')
    dates_invalides = df['date_commande'].isna().sum()
    df = df.dropna(subset=["date_commande"])
    logger.info(f"R2 dates : {dates_invalides} dates invalides supprimées")

    # R3 — Harmonisation des villes via référentiel
    mapping_villes = _load_ville_mapping()
    df['ville_livraison'] = df['ville_livraison'].str.strip().str.lower()
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')
    logger.info(f"R3 villes harmonisées : {df['ville_livraison'].nunique()} villes distinctes")

    # R4 — Standardisation des statuts (incluant 'retourné')
    mapping_statuts = {
        'livré': 'livré', 'livre': 'livré', 'LIVRE': 'livré', 'DONE': 'livré',
        'annulé': 'annulé', 'annule': 'annulé', 'KO': 'annulé',
        'en_cours': 'en_cours', 'OK': 'en_cours',
        'retourné': 'retourné', 'retourne': 'retourné', 'RETOURNE': 'retourné', 'returned': 'retourné'
    }
    df['statut'] = df['statut'].replace(mapping_statuts)
    invalides = ~df['statut'].isin(['livré', 'annulé', 'en_cours', 'retourné'])
    nb_invalides = invalides.sum()
    if nb_invalides:
        logger.warning(f"R4 statuts : {nb_invalides} valeurs non reconnues → 'inconnu'")
        df.loc[invalides, 'statut'] = 'inconnu'
    else:
        logger.info("R4 statuts : tous standardisés")

    # R5 — Quantités invalides
    avant = len(df)
    df['quantite'] = pd.to_numeric(df['quantite'], errors='coerce')
    df = df[df['quantite'] > 0]
    logger.info(f"R5 quantités : {avant - len(df)} lignes supprimées (quantité <= 0)")

    # R6 — Prix nuls (commandes test)
    avant = len(df)
    df['prix_unitaire'] = pd.to_numeric(df['prix_unitaire'], errors='coerce')
    df = df[df['prix_unitaire'] > 0]
    logger.info(f"R6 prix : {avant - len(df)} commandes test supprimées")

    # R7 — Livreurs manquants
    nb_manquants = df['id_livreur'].isna().sum()
    df['id_livreur'] = df['id_livreur'].fillna('-1')
    logger.info(f"R7 livreurs : {nb_manquants} valeurs manquantes remplacées par -1")

    logger.info(f"Commandes : {initial} → {len(df)} lignes ({initial - len(df)} supprimées au total)")
    return df