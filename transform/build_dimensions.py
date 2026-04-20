import pandas as pd
import logging

def build_dim_temps(date_debut='2022-01-01', date_fin='2025-12-31'):
    dates = pd.date_range(start=date_debut, end=date_fin)
    df = pd.DataFrame({'date_complete': dates})
    df['id_date'] = df['date_complete'].dt.strftime('%Y%m%d').astype(int)
    df['jour'] = df['date_complete'].dt.day
    df['mois'] = df['date_complete'].dt.month
    df['trimestre'] = df['date_complete'].dt.quarter
    df['annee'] = df['date_complete'].dt.year
    df['libelle_mois'] = df['date_complete'].dt.month_name()
    df['est_weekend'] = df['date_complete'].dt.dayofweek >= 5
    
    # Ramadan (Dates approximatives pour le Maroc)
    ramadan = [('2023-03-23', '2023-04-21'), ('2024-03-11', '2024-04-10'), ('2025-03-01', '2025-03-30')]
    df['periode_ramadan'] = False
    for start_r, end_r in ramadan:
        df.loc[df['date_complete'].between(start_r, end_r), 'periode_ramadan'] = True
    return df

def add_scd2_columns(df):
    """Ajoute les colonnes techniques pour le SCD Type 2"""
    df_scd = df.copy()
    df_scd['date_debut'] = pd.Timestamp.now().date()
    df_scd['date_fin'] = pd.to_datetime('9999-12-31').date()
    df_scd['est_actif'] = True
    return df_scd

def build_fait_ventes(df_cmd, dim_client, dim_produit, dim_region):
    logging.info("Construction de la table de faits (Mapping SK)...")
    
    # On récupère les Clés Techniques (SK) au lieu des IDs naturels
    fait = df_cmd.merge(dim_client[['id_client', 'id_client_sk']], on='id_client', how='inner')
    fait = fait.merge(dim_produit[['id_produit', 'id_produit_sk']], left_on='id_produit', right_on='id_produit_nk', how='inner')
    
    # Mapping Région (basé sur la ville de livraison harmonisée)
    fait = fait.merge(dim_region[['id_region', 'ville']], left_on='ville_livraison', right_on='ville', how='left')
    
    fait['id_date'] = fait['date_commande'].dt.strftime('%Y%m%d').astype(int)
    fait['montant_ht'] = fait['quantite'] * fait['prix_unitaire']
    fait['montant_ttc'] = fait['montant_ht'] * 1.20 # TVA 20%
    
    # Colonnes finales pour PostgreSQL
    cols = ['id_date', 'id_produit_sk', 'id_client_sk', 'id_region', 'id_livreur', 
            'quantite', 'montant_ht', 'montant_ttc', 'statut']
    return fait[cols].rename(columns={'quantite': 'quantite_vendue', 'statut': 'statut_commande'})