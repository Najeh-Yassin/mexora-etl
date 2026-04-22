import pandas as pd
import logging
from datetime import date

def build_dim_temps():
    logging.info("Génération de la dimension Temps avec périodes Ramadan...")
    dates = pd.date_range(start="2020-01-01", end="2026-12-31")
    df = pd.DataFrame({"date_full": dates})
    df["id_date"] = df["date_full"].dt.strftime("%Y%m%d").astype(int)
    df["jour"] = df["date_full"].dt.day
    df["mois"] = df["date_full"].dt.month
    df["trimestre"] = df["date_full"].dt.quarter
    df["annee"] = df["date_full"].dt.year
    df["semaine"] = df["date_full"].dt.isocalendar().week
    df["libelle_jour"] = df["date_full"].dt.day_name()
    df["libelle_mois"] = df["date_full"].dt.month_name()
    df["est_weekend"] = df["date_full"].dt.dayofweek >= 5
    df["est_ferie_maroc"] = False

    # Règle Ramadan (Dates réelles pour le Maroc)
    ramadan_dates = [('2023-03-23', '2023-04-21'), ('2024-03-11', '2024-04-10'), ('2025-03-01', '2025-03-30')]
    df["periode_ramadan"] = False
    for start_r, end_r in ramadan_dates:
        df.loc[df["date_full"].between(start_r, end_r), "periode_ramadan"] = True
    
    return df[["id_date", "jour", "mois", "trimestre", "annee", "semaine", "libelle_jour", "libelle_mois", "est_weekend", "est_ferie_maroc", "periode_ramadan"]]

def build_dimensions(data_dict):
    logging.info("--- CONSTRUCTION DU MODÈLE EN ÉTOILE ---")
    df_cmd = data_dict["commandes"].copy()
    df_cli = data_dict["clients"].copy()
    df_prod = data_dict["produits"].copy()
    df_reg = data_dict["regions"].copy()
    
    # 1. DIMENSION CLIENT (SCD2)
    dim_client = df_cli.copy()
    dim_client["id_client_nk"] = dim_client["id_client"]
    dim_client["id_client_sk"] = range(1, len(dim_client) + 1)
    dim_client["date_debut"], dim_client["date_fin"], dim_client["est_actif"] = date.today(), date(9999, 12, 31), True
    
    # 2. DIMENSION PRODUIT (SCD2)
    dim_produit = df_prod.rename(columns={"id_produit": "id_produit_nk", "nom": "nom_produit", "prix_catalogue": "prix_standard"})
    dim_produit["id_produit_sk"] = range(1, len(dim_produit) + 1)
    dim_produit["date_debut"], dim_produit["date_fin"], dim_produit["est_actif"] = date.today(), date(9999, 12, 31), True

    # 3. DIMENSION REGION
    dim_region = df_reg.rename(columns={"nom_ville_standard": "ville"})
    dim_region["id_region"] = range(1, len(dim_region) + 1)

    # 4. DIMENSION LIVREUR (Basée sur les commandes)
    dim_livreur = pd.DataFrame({"id_livreur_nk": df_cmd["id_livreur"].unique()})
    dim_livreur["id_livreur"] = range(1, len(dim_livreur) + 1)
    dim_livreur["nom_livreur"] = "Livreur " + dim_livreur["id_livreur_nk"].astype(str)
    dim_livreur["type_transport"], dim_livreur["zone_couverture"] = "Standard", "Maroc"

    # 5. TABLE DE FAITS (JOINTURES PROPRES SANS DOUBLONS)
    fait = df_cmd.copy()
    
    # Jointures pour récupérer les SK
    fait = fait.merge(dim_produit[['id_produit_nk', 'id_produit_sk']], left_on='id_produit', right_on='id_produit_nk', how='left')
    fait = fait.merge(dim_client[['id_client_nk', 'id_client_sk']], left_on='id_client', right_on='id_client_nk', how='left')
    fait = fait.merge(dim_region[['ville', 'id_region']], left_on='ville_livraison', right_on='ville', how='left')
    fait = fait.merge(dim_livreur[['id_livreur_nk', 'id_livreur']], left_on='id_livreur', right_on='id_livreur_nk', how='left', suffixes=('', '_sk'))

    # Nettoyage des colonnes (On ne garde que les SK et les mesures)
    fait["id_date"] = fait["date_commande"].dt.strftime('%Y%m%d').astype(int)
    fait["montant_ht"] = fait["quantite"] * fait["prix_unitaire"]
    fait["montant_ttc"] = fait["montant_ht"] * 1.20
    
    # Renommage pour correspondre au SQL de ton binôme
    fait = fait.rename(columns={
        "id_produit_sk": "id_produit",
        "id_client_sk": "id_client",
        "id_livreur_sk": "id_livreur",
        "quantite": "quantite_vendue",
        "statut": "statut_commande"
    })

    cols_faits = ["id_date", "id_produit", "id_client", "id_region", "id_livreur", "quantite_vendue", "montant_ht", "montant_ttc", "statut_commande"]
    
    return {
        "dim_region": dim_region,
        "dim_produit": dim_produit.drop(columns=["id_produit_nk"]), # Optionnel
        "dim_client": dim_client.drop(columns=["id_client", "id_client_nk"]),
        "dim_livreur": dim_livreur,
        "dim_temps": build_dim_temps(),
        "fait_ventes": fait[cols_faits].dropna(subset=["id_produit", "id_client"])
    }