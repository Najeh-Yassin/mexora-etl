import pandas as pd
import logging

def transform_commandes(df, df_regions):
    initial = len(df)
    
    # R1 - Doublons id_commande
    df = df.drop_duplicates(subset=['id_commande'], keep='last')
    
    # R2 - Dates mixtes
    df['date_commande'] = pd.to_datetime(df['date_commande'], format='mixed', dayfirst=True, errors='coerce')
    df = df.dropna(subset=['date_commande'])
    
    # R3 - Harmonisation Villes (via regions_maroc.csv)
    # On crée un mapping Insensible à la casse
    df_regions['nom_ville_standard'] = df_regions['nom_ville_standard'].str.strip()
    mapping_villes = df_regions.set_index(df_regions['nom_ville_standard'].str.lower())['nom_ville_standard'].to_dict()
    
    df['ville_livraison'] = df['ville_livraison'].str.strip().str.lower()
    df['ville_livraison'] = df['ville_livraison'].map(mapping_villes).fillna('Non renseignée')
    
    # R4 - Statuts
    map_statuts = {'livre': 'livré', 'DONE': 'livré', 'OK': 'en_cours', 'KO': 'annulé'}
    df['statut'] = df['statut'].replace(map_statuts)
    valid_statuts = ['livré', 'annulé', 'en_cours', 'retourné']
    df.loc[~df['statut'].isin(valid_statuts), 'statut'] = 'inconnu'
    
    # R5 & R6 - Filtres Quantité > 0 et Prix > 0
    df['quantite'] = pd.to_numeric(df['quantite'], errors='coerce')
    df['prix_unitaire'] = pd.to_numeric(df['prix_unitaire'], errors='coerce')
    df = df[(df['quantite'] > 0) & (df['prix_unitaire'] > 0)]
    
    # R7 - Livreur manquant
    df['id_livreur'] = df['id_livreur'].fillna('-1')
    
    logging.info(f"[TRANSFORM] Commandes : {initial} -> {len(df)} lignes.")
    return df