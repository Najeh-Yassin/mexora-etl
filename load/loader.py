import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

def get_engine():
    from config.settings import DB_URL
    engine = create_engine(DB_URL, pool_pre_ping=True)
    logger.info("[LOAD] Connexion PostgreSQL établie.")
    return engine

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    # ... (identique à votre version)
    return df

def charger_dimension(df: pd.DataFrame, table_name: str, engine, schema: str = "dwh_mexora", if_exists: str = "append") -> None:
    df = _prepare_df(df)
    try:
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists,
                  index=False, method="multi", chunksize=1000)
        logger.info(f"[LOAD] {table_name:20s} : {len(df):>6} lignes chargées.")
    except SQLAlchemyError as e:
        logger.error(f"[LOAD] ERREUR sur {table_name} : {e}")
        raise

def charger_faits(df: pd.DataFrame, engine, schema: str = "dwh_mexora", chunksize: int = 5000):
    """
    UPSERT sur fait_ventes en utilisant ON CONFLICT DO UPDATE.
    La contrainte d'unicité doit exister : par exemple UNIQUE (id_date, id_produit, id_client, id_region, id_livreur)
    ou utilisez id_commande comme clé naturelle (ajoutez-la dans la table).
    Ici on suppose que vous avez ajouté une colonne id_commande_nk dans fait_ventes et une contrainte unique.
    """
    df = _prepare_df(df)
    if df.empty:
        logger.warning("[LOAD] Aucune ligne à charger pour fait_ventes")
        return

    # Copie pour éviter de modifier l'original
    data = df.to_dict(orient='records')

    # Définition de la table SQLAlchemy (pour l'upsert)
    from sqlalchemy import Table, MetaData, Column, Integer, String, Numeric, Date, SmallInteger, Boolean, BigInteger
    metadata = MetaData()
    # On ne définit que les colonnes nécessaires pour l'upsert, mais on peut aussi charger la table existante
    # Méthode simple : utiliser text() avec ON CONFLICT
    # Mais pour rester dans l'esprit SQLAlchemy, on va utiliser insert() avec on_conflict_do_update

    # Récupérer la réflexion de la table existante
    table_fait_ventes = Table('fait_ventes', metadata, autoload_with=engine, schema=schema)

    # On suppose qu'il existe une contrainte unique sur (id_date, id_produit, id_client, id_region) par exemple.
    # Si ce n'est pas le cas, créez-la dans create_dwh.sql :
    # ALTER TABLE dwh_mexora.fait_ventes ADD CONSTRAINT fait_ventes_unique UNIQUE (id_date, id_produit, id_client, id_region);

    with engine.begin() as conn:
        for i in range(0, len(data), chunksize):
            chunk = data[i:i+chunksize]
            stmt = insert(table_fait_ventes).values(chunk)
            # Mise à jour de toutes les colonnes sauf la PK
            update_dict = {c.name: stmt.excluded[c.name] for c in table_fait_ventes.columns if c.name != 'id_vente'}
            stmt = stmt.on_conflict_do_update(
                constraint='fait_ventes_unique',  # nom de la contrainte unique
                set_=update_dict
            )
            conn.execute(stmt)
            logger.debug(f"[LOAD] fait_ventes : chunk {i//chunksize + 1} inséré/upserté")

    logger.info(f"[LOAD] fait_ventes : {len(df)} lignes traitées (UPSERT)")

def load_data(star_schema: dict) -> None:
    engine = get_engine()
    ordre_chargement = ["dim_temps", "dim_region", "dim_produit", "dim_client", "dim_livreur", "fait_ventes"]

    logger.info("--- DÉBUT DU CHARGEMENT ---")
    with engine.begin() as conn:
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
        conn.execute(text("SET session_replication_role = 'origin';"))

    logger.info("--- CHARGEMENT TERMINÉ AVEC SUCCÈS ---")