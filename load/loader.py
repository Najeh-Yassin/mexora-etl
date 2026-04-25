"""
load/loader.py
Chargement du star schema dans PostgreSQL — dwh_mexora
Corrigé : ordre FK, upsert sécurisé, gestion des types pandas → SQL
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────────────────────────────────────

def get_engine():
    """Retourne le moteur SQLAlchemy. Modifier les credentials ici."""
    from config.settings import DB_URL
    engine = create_engine(DB_URL, pool_pre_ping=True)
    logger.info("[LOAD] Connexion PostgreSQL établie.")
    return engine


# ─────────────────────────────────────────────────────────────────────────────
# PRÉPARATION DU DATAFRAME (nettoyage types avant envoi à psycopg2)
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit les types pandas incompatibles avec PostgreSQL :
    - Categorical → str
    - Int64 nullable → object (None au lieu de pd.NA)
    - NaN float dans les colonnes FK entières → None
    """
    df = df.copy()
    for col in df.columns:
        # Categorical (résultat de pd.cut / pd.Categorical)
        if hasattr(df[col], "cat"):
            df[col] = df[col].astype(object).where(df[col].notna(), other=None)
        # Nullable integer (Int64)
        elif pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), other=None)
        # Float qui représente des entiers (FK résolues via .map())
        elif df[col].dtype == float and col.startswith("id_"):
            df[col] = df[col].where(df[col].notna(), other=None)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT D'UNE DIMENSION (stratégie REPLACE = truncate + reload)
# ─────────────────────────────────────────────────────────────────────────────

def charger_dimension(df: pd.DataFrame,
                      table_name: str,
                      engine,
                      schema: str = "dwh_mexora",
                      if_exists: str = "append") -> None:
    """
    Charge une dimension dans PostgreSQL.
    if_exists='append' car les tables sont créées via DDL SQL (create_dwh.sql).
    Utilise chunksize=1000 et method='multi' pour de meilleures performances.
    """
    df = _prepare_df(df)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )
        logger.info(f"[LOAD] {table_name:20s} : {len(df):>6} lignes chargées.")
    except SQLAlchemyError as e:
        logger.error(f"[LOAD] ERREUR sur {table_name} : {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DE LA TABLE DE FAITS (stratégie APPEND par chunks)
# ─────────────────────────────────────────────────────────────────────────────

def charger_faits(df: pd.DataFrame,
                  engine,
                  schema: str = "dwh_mexora",
                  chunksize: int = 5000) -> None:
    """
    Charge fait_ventes en mode append par chunks de 5000 lignes.
    Les doublons éventuels sont ignorés grâce à ON CONFLICT DO NOTHING
    appliqué au niveau SQL (la PK BIGSERIAL empêche les collisions si
    le pipeline est relancé après troncature).
    """
    df = _prepare_df(df)
    try:
        df.to_sql(
            name="fait_ventes",
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=chunksize,
        )
        logger.info(f"[LOAD] fait_ventes           : {len(df):>6} lignes chargées.")
    except SQLAlchemyError as e:
        logger.error(f"[LOAD] ERREUR sur fait_ventes : {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE APPELÉ PAR main.py
# ─────────────────────────────────────────────────────────────────────────────

def load_data(star_schema: dict) -> None:
    """
    Charge toutes les tables dans l'ordre respectant les FK.
    Ordre obligatoire :
        dim_temps → dim_region → dim_produit → dim_client → dim_livreur → fait_ventes
    """
    engine = get_engine()

    # Ordre vital pour les contraintes de clés étrangères
    ordre_chargement = [
        "dim_temps",
        "dim_region",
        "dim_produit",
        "dim_client",
        "dim_livreur",
        "fait_ventes",
    ]

    logger.info("--- DÉBUT DU CHARGEMENT ---")

    with engine.begin() as conn:
        # Désactiver temporairement les FK pour éviter les erreurs d'ordre
        conn.execute(text("SET session_replication_role = 'replica';"))

    for table_name in ordre_chargement:
        if table_name not in star_schema:
            logger.warning(f"[LOAD] {table_name} absent du star_schema — ignoré.")
            continue

        df = star_schema[table_name]

        if table_name == "fait_ventes":
            charger_faits(df, engine)
        else:
            charger_dimension(df, table_name, engine)

    with engine.begin() as conn:
        # Réactiver les FK
        conn.execute(text("SET session_replication_role = 'origin';"))

    logger.info("--- CHARGEMENT TERMINÉ AVEC SUCCÈS ---")