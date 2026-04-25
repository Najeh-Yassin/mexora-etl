"""
transform/build_dimensions.py
Construction du schéma en étoile — dwh_mexora
Corrigé : nom_complet, tranche_age, region_admin, segment_client, types SCD
"""

import pandas as pd
import numpy as np
import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _safe_str(series: pd.Series) -> pd.Series:
    """Convertit une Series en str en gérant NaN/None/Categorical."""
    if hasattr(series, "cat"):          # pandas Categorical (résultat de pd.cut)
        series = series.astype(object)
    return series.where(series.notna(), other=None).astype(object)


# ──────────────────────────────────────────────────────────────────────────────
# DIM_TEMPS
# ──────────────────────────────────────────────────────────────────────────────

def build_dim_temps(date_debut: str = "2020-01-01",
                    date_fin: str   = "2026-12-31") -> pd.DataFrame:
    """
    Génère la dimension temporelle complète.
    id_date au format YYYYMMDD (INTEGER — clé primaire PostgreSQL).
    """
    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")

    feries_maroc = {
        # 2022
        "2022-01-01","2022-01-11","2022-05-01","2022-07-30",
        "2022-08-14","2022-11-06","2022-11-18",
        # 2023
        "2023-01-01","2023-01-11","2023-05-01","2023-07-30",
        "2023-08-14","2023-11-06","2023-11-18",
        # 2024
        "2024-01-01","2024-01-11","2024-05-01","2024-07-30",
        "2024-08-14","2024-11-06","2024-11-18",
        # 2025
        "2025-01-01","2025-01-11","2025-05-01","2025-07-30",
        "2025-08-14","2025-11-06","2025-11-18",
    }

    ramadan_periodes = [
        ("2022-04-02", "2022-05-01"),
        ("2023-03-22", "2023-04-20"),
        ("2024-03-10", "2024-04-09"),
        ("2025-03-01", "2025-03-30"),
    ]

    df = pd.DataFrame({
        "id_date":       dates.strftime("%Y%m%d").astype(int),
        "date_complete": dates.date,                          # Python date object
        "jour":          dates.day.astype("int16"),
        "mois":          dates.month.astype("int16"),
        "trimestre":     dates.quarter.astype("int16"),
        "annee":         dates.year.astype("int16"),
        "semaine":       dates.isocalendar().week.astype("int16"),
        "libelle_jour":  dates.strftime("%A"),
        "libelle_mois":  dates.strftime("%B"),
        "est_weekend":   dates.dayofweek >= 5,
        "est_ferie_maroc": dates.strftime("%Y-%m-%d").isin(feries_maroc),
        "periode_ramadan": False,
    })

    for debut, fin in ramadan_periodes:
        mask = (df["date_complete"] >= pd.Timestamp(debut).date()) & \
               (df["date_complete"] <= pd.Timestamp(fin).date())
        df.loc[mask, "periode_ramadan"] = True

    logger.info(f"[BUILD] dim_temps : {len(df)} lignes générées ({date_debut} → {date_fin})")
    return df[[
        "id_date", "jour", "mois", "trimestre", "annee", "semaine",
        "libelle_jour", "libelle_mois", "est_weekend",
        "est_ferie_maroc", "periode_ramadan"
    ]]


# ──────────────────────────────────────────────────────────────────────────────
# DIM_CLIENT  ← ROOT CAUSE DU CRASH
# ──────────────────────────────────────────────────────────────────────────────

def build_dim_client(df_clients: pd.DataFrame,
                     df_commandes: pd.DataFrame,
                     df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_CLIENT avec toutes les colonnes requises par le schéma SQL :
      id_client_nk, nom_complet, tranche_age, sexe, ville, region_admin,
      segment_client, canal_acquisition, date_debut, date_fin, est_actif

    CORRECTIONS APPLIQUÉES :
    ─────────────────────────
    BUG 1 — nom_complet manquant
        clean_clients.py ne concatène jamais nom + prenom.
        → On le construit ici depuis les colonnes brutes.

    BUG 2 — region_admin manquante
        La colonne region_admin vient du référentiel regions_maroc.csv.
        clean_clients.py ne fait jamais ce join.
        → On joint df_regions sur ville normalisée ici.

    BUG 3 — segment_client absent du DataFrame clients
        calculer_segments_clients() retourne un DataFrame séparé.
        Il faut merger sur id_client.
        → On appelle calculer_segments_clients() ici et on merge.

    BUG 4 — tranche_age de type Categorical (pandas.cut)
        PostgreSQL ne comprend pas le type Categorical.
        → On cast en str via _safe_str() avant le chargement.
    """
    df = df_clients.copy()

    # ── BUG 1 — Construire nom_complet ────────────────────────────────────────
    if "nom_complet" not in df.columns:
        nom    = df.get("nom",    pd.Series("", index=df.index)).fillna("")
        prenom = df.get("prenom", pd.Series("", index=df.index)).fillna("")
        df["nom_complet"] = (nom.str.strip() + " " + prenom.str.strip()).str.strip()
        df["nom_complet"] = df["nom_complet"].replace("", None)
        logger.info("[BUILD] BUG 1 corrigé — nom_complet construit depuis nom + prenom")

    # ── BUG 2 — Joindre region_admin depuis le référentiel géographique ───────
    if "region_admin" not in df.columns:
        if df_regions is not None and len(df_regions) > 0:
            # Normalisation de la clé de jointure
            ref = df_regions[["nom_ville_standard", "region_admin"]].copy()
            ref["_ville_norm"] = ref["nom_ville_standard"].str.lower().str.strip()
            ref = ref.drop_duplicates(subset=["_ville_norm"])

            df["_ville_norm"] = df.get("ville", pd.Series(dtype=str)) \
                                   .astype(str).str.lower().str.strip()
            df = df.merge(ref[["_ville_norm", "region_admin"]],
                          on="_ville_norm", how="left")
            df.drop(columns=["_ville_norm"], inplace=True)
            logger.info("[BUILD] BUG 2 corrigé — region_admin jointu depuis df_regions")
        else:
            df["region_admin"] = None
            logger.warning("[BUILD] BUG 2 — df_regions vide, region_admin = NULL")

    # ── BUG 3 — Calculer et merger segment_client ─────────────────────────────
    if "segment_client" not in df.columns:
        segments = calculer_segments_clients(df_commandes)
        df = df.merge(segments[["id_client", "segment_client"]],
                      on="id_client", how="left")
        df["segment_client"] = df["segment_client"].fillna("Bronze")
        logger.info("[BUILD] BUG 3 corrigé — segment_client mergé depuis commandes")

    # ── BUG 4 — Convertir tranche_age Categorical → str ──────────────────────
    if "tranche_age" in df.columns:
        df["tranche_age"] = _safe_str(df["tranche_age"])
        logger.info("[BUILD] BUG 4 corrigé — tranche_age converti en str")
    else:
        df["tranche_age"] = None

    # ── Colonnes SCD Type 1 (schéma PostgreSQL) ───────────────────────────────
    df["date_debut"] = date.today()
    df["date_fin"]   = date(9999, 12, 31)
    df["est_actif"]  = True

    # ── Sélection finale (ordre = ordre DDL) ──────────────────────────────────
    cols_finales = [
        "id_client",          # → id_client_nk dans PostgreSQL
        "nom_complet",
        "tranche_age",
        "sexe",
        "ville",
        "region_admin",
        "segment_client",
        "canal_acquisition",
        "date_debut",
        "date_fin",
        "est_actif",
    ]
    # Garder uniquement les colonnes existantes (sécurité)
    cols_presentes = [c for c in cols_finales if c in df.columns]
    df_final = df[cols_presentes].copy()

    # Renommage pour correspondre au DDL PostgreSQL
    df_final = df_final.rename(columns={"id_client": "id_client_nk"})

    logger.info(f"[BUILD] dim_client : {len(df_final)} lignes, "
                f"colonnes : {list(df_final.columns)}")
    return df_final


# ──────────────────────────────────────────────────────────────────────────────
# DIM_PRODUIT  (SCD Type 2)
# ──────────────────────────────────────────────────────────────────────────────

def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_PRODUIT avec colonnes SCD Type 2.
    Normalise la catégorie en title-case.
    Gère les prix_catalogue à null.
    """
    df = df_produits.copy()

    # Normalisation catégorie
    if "categorie" in df.columns:
        df["categorie"] = df["categorie"].str.strip().str.title()
    if "sous_categorie" in df.columns:
        df["sous_categorie"] = df["sous_categorie"].str.strip().str.title()

    # Prix null → 0
    if "prix_catalogue" in df.columns:
        df["prix_standard"] = pd.to_numeric(df["prix_catalogue"], errors="coerce").fillna(0.0)
    else:
        df["prix_standard"] = 0.0

    # SCD Type 2 columns
    df["date_debut"] = date.today()
    df["date_fin"]   = date(9999, 12, 31)
    df["est_actif"]  = True

    rename_map = {
        "id_produit": "id_produit_nk",
        "nom":        "nom_produit",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    cols_finales = [
        "id_produit_nk", "nom_produit", "categorie", "sous_categorie",
        "marque", "fournisseur", "prix_standard", "origine_pays",
        "date_debut", "date_fin", "est_actif",
    ]
    cols_presentes = [c for c in cols_finales if c in df.columns]
    df_final = df[cols_presentes].copy()

    logger.info(f"[BUILD] dim_produit : {len(df_final)} lignes")
    return df_final


# ──────────────────────────────────────────────────────────────────────────────
# DIM_REGION
# ──────────────────────────────────────────────────────────────────────────────

def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_REGION depuis le référentiel régions_maroc.csv.
    Ce fichier est déjà propre selon l'énoncé.
    """
    df = df_regions.copy()

    rename_map = {
        "nom_ville_standard": "ville",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df["pays"] = "Maroc"

    cols_finales = ["ville", "province", "region_admin", "zone_geo", "pays"]
    cols_presentes = [c for c in cols_finales if c in df.columns]
    df_final = df[cols_presentes].drop_duplicates(subset=["ville"]).copy()

    logger.info(f"[BUILD] dim_region : {len(df_final)} lignes")
    return df_final


# ──────────────────────────────────────────────────────────────────────────────
# DIM_LIVREUR
# ──────────────────────────────────────────────────────────────────────────────

def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_LIVREUR depuis les commandes (id_livreur unique).
    Ajoute la ligne id=-1 pour livreur inconnu (valeur de remplacement R7).
    """
    livreurs = (
        df_commandes[["id_livreur"]]
        .drop_duplicates()
        .dropna()
        .copy()
    )
    livreurs = livreurs[livreurs["id_livreur"] != "-1"]   # exclure le sentinel
    livreurs = livreurs.rename(columns={"id_livreur": "id_livreur_nk"})
    livreurs["nom_livreur"]    = None
    livreurs["type_transport"] = None
    livreurs["zone_couverture"] = None

    # Ligne sentinel pour livreur inconnu
    row_inconnu = pd.DataFrame([{
        "id_livreur_nk":  "-1",
        "nom_livreur":    "Livreur inconnu",
        "type_transport": "inconnu",
        "zone_couverture": None,
    }])

    df_final = pd.concat([row_inconnu, livreurs], ignore_index=True)
    logger.info(f"[BUILD] dim_livreur : {len(df_final)} lignes (dont 1 sentinel inconnu)")
    return df_final


# ──────────────────────────────────────────────────────────────────────────────
# SEGMENTATION CLIENT (helper réutilisé dans build_dim_client)
# ──────────────────────────────────────────────────────────────────────────────

def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule Gold / Silver / Bronze basé sur le CA livré des 12 derniers mois.
    Règles Mexora :
        Gold   : CA >= 15 000 MAD
        Silver : CA >= 5 000 MAD
        Bronze : CA <  5 000 MAD
    """
    date_limite = pd.Timestamp(date.today() - timedelta(days=365))

    df_recents = df_commandes[
        (df_commandes["date_commande"] >= date_limite) &
        (df_commandes["statut"] == "livré")
    ].copy()

    df_recents["montant_ttc"] = (
        pd.to_numeric(df_recents["quantite"],     errors="coerce").fillna(0) *
        pd.to_numeric(df_recents["prix_unitaire"], errors="coerce").fillna(0)
    )

    ca = df_recents.groupby("id_client")["montant_ttc"].sum().reset_index()
    ca.columns = ["id_client", "ca_12m"]

    def segmenter(ca_val: float) -> str:
        if ca_val >= 15_000:
            return "Gold"
        if ca_val >= 5_000:
            return "Silver"
        return "Bronze"

    ca["segment_client"] = ca["ca_12m"].apply(segmenter)
    return ca[["id_client", "segment_client", "ca_12m"]]


# ──────────────────────────────────────────────────────────────────────────────
# FAIT_VENTES
# ──────────────────────────────────────────────────────────────────────────────

def build_fait_ventes(df_commandes: pd.DataFrame,
                      dim_temps:    pd.DataFrame,
                      dim_client:   pd.DataFrame,
                      dim_produit:  pd.DataFrame,
                      dim_region:   pd.DataFrame,
                      dim_livreur:  pd.DataFrame) -> pd.DataFrame:
    """
    Construit FAIT_VENTES en résolvant toutes les clés étrangères.
    Calcule : montant_ht, montant_ttc, delai_livraison_jours.
    """
    df = df_commandes.copy()

    # ── Résolution id_date ────────────────────────────────────────────────────
    df["id_date"] = pd.to_datetime(df["date_commande"]).dt.strftime("%Y%m%d").astype(int)

    # ── Résolution id_client (via id_client_nk) ───────────────────────────────
    map_client = dim_client.reset_index().rename(columns={"index": "id_client_sk"})
    map_client = dict(zip(map_client["id_client_nk"],
                          map_client.index + 1))   # surrogate key 1-based
    df["id_client"] = df["id_client"].map(map_client)

    # ── Résolution id_produit (via id_produit_nk) ─────────────────────────────
    map_produit = dict(zip(dim_produit["id_produit_nk"],
                           range(1, len(dim_produit) + 1)))
    df["id_produit"] = df["id_produit"].map(map_produit)

    # ── Résolution id_region (via ville_livraison) ────────────────────────────
    # dim_region a déjà un index SERIAL dans PostgreSQL ;
    # ici on crée un proxy numérique local basé sur la position
    dim_region_idx = dim_region.reset_index(drop=True)
    dim_region_idx["id_region"] = dim_region_idx.index + 1
    map_region = dict(zip(
        dim_region_idx["ville"].str.lower().str.strip(),
        dim_region_idx["id_region"]
    ))
    df["id_region"] = df["ville_livraison"].str.lower().str.strip().map(map_region)

    # ── Résolution id_livreur ─────────────────────────────────────────────────
    dim_livreur_idx = dim_livreur.reset_index(drop=True)
    dim_livreur_idx["id_livreur"] = dim_livreur_idx.index + 1
    map_livreur = dict(zip(dim_livreur_idx["id_livreur_nk"],
                           dim_livreur_idx["id_livreur"]))
    df["id_livreur"] = df["id_livreur"].astype(str).map(map_livreur)

    # ── Calculs financiers ────────────────────────────────────────────────────
    df["quantite_vendue"] = pd.to_numeric(df["quantite"],      errors="coerce").fillna(0).astype(int)
    df["prix_u"]          = pd.to_numeric(df["prix_unitaire"], errors="coerce").fillna(0.0)
    df["remise_pct"]      = 0.0                                  # non disponible dans la source
    TVA = 0.20
    df["montant_ht"]  = (df["quantite_vendue"] * df["prix_u"] * (1 - df["remise_pct"])).round(2)
    df["montant_ttc"] = (df["montant_ht"] * (1 + TVA)).round(2)

    # ── Délai de livraison ────────────────────────────────────────────────────
    if "date_livraison" in df.columns:
        df["date_livraison"] = pd.to_datetime(df["date_livraison"], errors="coerce")
        df["delai_livraison_jours"] = (
            (df["date_livraison"] - df["date_commande"]).dt.days
            .clip(lower=0)
            .astype("Int64")   # nullable integer (None pour valeurs manquantes)
        )
    else:
        df["delai_livraison_jours"] = None

    # ── Rejet des lignes avec FK manquante (sauf livreur) ────────────────────
    fk_obligatoires = ["id_date", "id_client", "id_produit", "id_region"]
    mask_invalide = df[fk_obligatoires].isna().any(axis=1)
    nb_rejetees = mask_invalide.sum()
    if nb_rejetees:
        logger.warning(f"[BUILD] fait_ventes — {nb_rejetees} lignes rejetées (FK manquante)")
    df = df[~mask_invalide].copy()

    # ── Sélection finale ──────────────────────────────────────────────────────
    cols_finales = [
        "id_date", "id_produit", "id_client", "id_region", "id_livreur",
        "quantite_vendue", "montant_ht", "montant_ttc",
        "delai_livraison_jours", "remise_pct", "statut",
    ]
    cols_presentes = [c for c in cols_finales if c in df.columns]
    df_final = df[cols_presentes].rename(columns={"statut": "statut_commande"})
    df_final["date_chargement"] = pd.Timestamp.now()

    logger.info(f"[BUILD] fait_ventes : {len(df_final)} lignes construites")
    return df_final


# ──────────────────────────────────────────────────────────────────────────────
# ORCHESTRATEUR PRINCIPAL (appelé par main.py)
# ──────────────────────────────────────────────────────────────────────────────

def build_dimensions(cleaned_data: dict) -> dict:
    """
    Point d'entrée appelé par main.py.
    Reçoit cleaned_data avec les clés :
        'commandes', 'clients', 'produits', 'regions'
    Retourne un dict avec toutes les tables du star schema.
    """
    logger.info("--- CONSTRUCTION DU MODÈLE EN ÉTOILE (SÉCURISÉE) ---")

    df_commandes = cleaned_data["commandes"]
    df_clients   = cleaned_data["clients"]
    df_produits  = cleaned_data["produits"]
    df_regions   = cleaned_data["regions"]

    # 1. Dimensions indépendantes
    dim_temps   = build_dim_temps()
    dim_region  = build_dim_region(df_regions)
    dim_produit = build_dim_produit(df_produits)
    dim_livreur = build_dim_livreur(df_commandes)

    # 2. DIM_CLIENT (dépend de commandes + regions — corrigé)
    dim_client = build_dim_client(df_clients, df_commandes, df_regions)

    # 3. Table de faits (dépend de toutes les dimensions)
    fait_ventes = build_fait_ventes(
        df_commandes, dim_temps, dim_client,
        dim_produit, dim_region, dim_livreur
    )

    star_schema = {
        "dim_temps":    dim_temps,
        "dim_region":   dim_region,
        "dim_produit":  dim_produit,
        "dim_livreur":  dim_livreur,
        "dim_client":   dim_client,
        "fait_ventes":  fait_ventes,
    }

    for name, df in star_schema.items():
        logger.info(f"[BUILD] ✓ {name:15s} → {len(df):>6} lignes | "
                    f"colonnes : {list(df.columns)}")

    return star_schema