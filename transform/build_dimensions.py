import pandas as pd
import logging
from datetime import date

def build_dim_temps():
    """Génère la dimension temps alignée sur le script SQL."""
    logging.info("Génération de la dimension Temps...")
    dates = pd.date_range(start="2020-01-01", end="2026-12-31")
    df = pd.DataFrame({"date_full": dates})
    
    # Calculs des attributs
    df["id_date"] = df["date_full"].dt.strftime("%Y%m%d").astype(int)
    df["jour"] = df["date_full"].dt.day.astype(int)
    df["mois"] = df["date_full"].dt.month.astype(int)
    df["trimestre"] = df["date_full"].dt.quarter.astype(int)
    df["annee"] = df["date_full"].dt.year.astype(int)
    df["semaine"] = df["date_full"].dt.isocalendar().week.astype(int)
    df["libelle_jour"] = df["date_full"].dt.day_name()
    df["libelle_mois"] = df["date_full"].dt.month_name()
    df["est_weekend"] = df["date_full"].dt.dayofweek >= 5
    df["est_ferie_maroc"] = False
    df["periode_ramadan"] = False
    
    # Sélection stricte des colonnes présentes dans la table SQL dim_temps
    cols_sql = [
        "id_date", "jour", "mois", "trimestre", "annee", "semaine", 
        "libelle_jour", "libelle_mois", "est_weekend", "est_ferie_maroc", "periode_ramadan"
    ]
    return df[cols_sql]

def build_dimensions(data_dict):
    logging.info("--- CONSTRUCTION ET ALIGNEMENT FINAL DES DONNÉES ---")
    
    df_cmd = data_dict["commandes"].copy()
    df_cli = data_dict["clients"].copy()
    df_prod = data_dict["produits"].copy()
    df_reg = data_dict["regions"].copy()
    
    date_auj = date.today()
    date_eternite = date(9999, 12, 31)

    # 1. DIMENSION REGION
    dim_region = df_reg.rename(columns={"nom_ville_standard": "ville"})
    if "pays" not in dim_region.columns: dim_region["pays"] = "Maroc"
    dim_region = dim_region[["ville", "province", "region_admin", "zone_geo", "pays"]].drop_duplicates()
    # On crée un ID de jointure interne (non envoyé à SQL car id_region est SERIAL)
    dim_region["id_region_tmp"] = range(1, len(dim_region) + 1)

    # 2. DIMENSION PRODUIT (SCD2)
    dim_produit = df_prod.rename(columns={"nom": "nom_produit", "prix_catalogue": "prix_standard"})
    dim_produit["id_produit_nk"] = dim_produit["id_produit"]
    dim_produit["id_produit_sk"] = range(1, len(dim_produit) + 1)
    dim_produit["date_debut"], dim_produit["date_fin"], dim_produit["est_actif"] = date_auj, date_eternite, True
    
    for col in ["sous_categorie", "marque", "fournisseur", "origine_pays"]:
        if col not in dim_produit.columns: dim_produit[col] = "Inconnu"
    
    cols_prod_final = ["id_produit_sk", "id_produit_nk", "nom_produit", "categorie", "sous_categorie", 
                       "marque", "fournisseur", "prix_standard", "origine_pays", "date_debut", "date_fin", "est_actif"]
    dim_produit_final = dim_produit[cols_prod_final]

    # 3. DIMENSION CLIENT (SCD2)
    dim_client = df_cli.copy()
    dim_client["id_client_nk"] = dim_client["id_client"]
    dim_client["id_client_sk"] = range(1, len(dim_client) + 1)
    dim_client["nom_complet"] = dim_client["prenom"].fillna('') + " " + dim_client["nom"].fillna('')
    dim_client["date_debut"], dim_client["date_fin"], dim_client["est_actif"] = date_auj, date_eternite, True
    
    for col in ["segment_client", "tranche_age", "region_admin", "canal_acquisition"]:
        if col not in dim_client.columns: dim_client[col] = "Inconnu"

    cols_cli_final = ["id_client_sk", "id_client_nk", "nom_complet", "tranche_age", "sexe", "ville", 
                      "region_admin", "segment_client", "canal_acquisition", "date_debut", "date_fin", "est_actif"]
    dim_client_final = dim_client[cols_cli_final]

    # 4. DIMENSION LIVREUR
    dim_livreur = pd.DataFrame({"id_livreur_nk": df_cmd["id_livreur"].unique()})
    dim_livreur["id_livreur"] = range(1, len(dim_livreur) + 1)
    dim_livreur["nom_livreur"] = "Livreur " + dim_livreur["id_livreur_nk"].astype(str)
    dim_livreur["type_transport"], dim_livreur["zone_couverture"] = "Standard", "Maroc"
    dim_livreur_final = dim_livreur[["id_livreur", "id_livreur_nk", "nom_livreur", "type_transport", "zone_couverture"]]

    # 5. TABLE DE FAITS (FAIT_VENTES)
    fait_ventes = df_cmd.copy()
    
    # Jointures pour récupérer les SK (Surrogate Keys) numériques
    fait_ventes = fait_ventes.merge(dim_produit[['id_produit_nk', 'id_produit_sk']], left_on='id_produit', right_on='id_produit_nk', how='left')
    fait_ventes = fait_ventes.merge(dim_client[['id_client_nk', 'id_client_sk']], left_on='id_client', right_on='id_client_nk', how='left')
    fait_ventes = fait_ventes.merge(dim_region[['ville', 'id_region_tmp']], left_on='ville_livraison', right_on='ville', how='left')
    fait_ventes = fait_ventes.merge(dim_livreur[['id_livreur_nk', 'id_livreur']], left_on='id_livreur', right_on='id_livreur_nk', how='left', suffixes=('', '_sk'))

    # Nettoyage des colonnes : on supprime les anciens noms pour éviter "DuplicateColumnError"
    cols_a_supprimer = ['id_produit', 'id_client', 'id_livreur', 'id_produit_nk', 'id_client_nk', 'id_livreur_nk', 'ville']
    fait_ventes = fait_ventes.drop(columns=[c for c in cols_a_supprimer if c in fait_ventes.columns])

    # Renommage vers les noms de colonnes SQL
    fait_ventes = fait_ventes.rename(columns={
        "id_produit_sk": "id_produit",
        "id_client_sk": "id_client",
        "id_region_tmp": "id_region",
        "id_livreur_sk": "id_livreur",
        "quantite": "quantite_vendue"
    })

    # Calcul des mesures
    fait_ventes["id_date"] = fait_ventes["date_commande"].dt.strftime('%Y%m%d').astype(int)
    fait_ventes["montant_ht"] = fait_ventes["quantite_vendue"].astype(float) * fait_ventes["prix_unitaire"].astype(float)
    fait_ventes["montant_ttc"] = fait_ventes["montant_ht"] * 1.20
    fait_ventes["statut_commande"] = fait_ventes["statut"]

    # Sélection finale stricte pour correspondre au SQL
    cols_sql_faits = ["id_date", "id_produit", "id_client", "id_region", "id_livreur", "quantite_vendue", "montant_ht", "montant_ttc", "statut_commande"]
    fait_ventes_final = fait_ventes[cols_sql_faits].dropna(subset=["id_produit", "id_client"])

    return {
        "dim_region": dim_region.drop(columns=["id_region_tmp"]), # id_region est SERIAL côté SQL
        "dim_produit": dim_produit_final,
        "dim_client": dim_client_final,
        "dim_livreur": dim_livreur_final,
        "dim_temps": build_dim_temps(),
        "fait_ventes": fait_ventes_final
    }