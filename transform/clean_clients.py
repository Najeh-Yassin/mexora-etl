import pandas as pd
import logging
from datetime import datetime, timedelta

def transform_clients(df_clients, df_cmd_clean):
    # R1 - Doublons email
    df_clients['email'] = df_clients['email'].str.lower().str.strip()
    df_clients = df_clients.drop_duplicates(subset=['email'], keep='last')
    
    # R2 - Sexe
    map_sexe = {'1': 'm', '0': 'f', 'homme': 'm', 'femme': 'f', 'm': 'm', 'f': 'f'}
    df_clients['sexe'] = df_clients['sexe'].str.lower().map(map_sexe).fillna('inconnu')
    
    # R3 - Age (16-100 ans)
    df_clients['date_naissance'] = pd.to_datetime(df_clients['date_naissance'], errors='coerce')
    df_clients['age'] = (pd.Timestamp.now() - df_clients['date_naissance']).dt.days // 365
    df_clients = df_clients[(df_clients['age'] >= 16) & (df_clients['age'] <= 100)]
    
    # R4 - Segmentation CA (12 derniers mois uniquement)
    # On définit la date limite (aujourd'hui - 365 jours)
    date_limite = pd.Timestamp.now() - pd.Timedelta(days=365)
    
    df_cmd_clean['ca'] = df_cmd_clean['quantite'].astype(float) * df_cmd_clean['prix_unitaire'].astype(float)
    
    # Filtre sur les 12 derniers mois + Statut livré
    mask_12m = (df_cmd_clean['date_commande'] >= date_limite) & (df_cmd_clean['statut'] == 'livré')
    ca_par_client = df_cmd_clean[mask_12m].groupby('id_client')['ca'].sum().reset_index()
    
    def get_segment(ca):
        if ca >= 15000: return 'Gold'
        if ca >= 5000: return 'Silver'
        return 'Bronze'
    
    ca_par_client['segment_client'] = ca_par_client['ca'].apply(get_segment)
    
    # Jointure avec les clients
    df_clients = df_clients.merge(ca_par_client[['id_client', 'segment_client']], on='id_client', how='left')
    df_clients['segment_client'] = df_clients['segment_client'].fillna('Bronze')
    
    logging.info(f"[TRANSFORM] Clients : Segmentation calculée sur les 12 derniers mois.")
    return df_clients