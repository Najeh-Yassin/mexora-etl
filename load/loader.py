from sqlalchemy import create_engine
from config.settings import DB_URL, DB_SCHEMA
import logging

def load_data(star_schema):
    """
    Charge les DataFrames dans PostgreSQL en respectant l'ordre des contraintes.
    """
    logging.info("--- PHASE DE CHARGEMENT (LOAD) ---")
    engine = create_engine(DB_URL)
    
    # ORDRE CRUCIAL : Les dimensions d'abord, la table de faits en dernier
    tables_order = [
        "dim_temps", 
        "dim_region", 
        "dim_produit", 
        "dim_client", 
        "dim_livreur", 
        "fait_ventes" # Toujours en dernier
    ]
    
    try:
        for table_name in tables_order:
            if table_name in star_schema:
                df = star_schema[table_name]
                logging.info(f"Chargement de {table_name} : {len(df)} lignes...")
                
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=DB_SCHEMA,
                    if_exists="append", # On ajoute les données
                    index=False,
                    method='multi', # Plus rapide pour les gros volumes
                    chunksize=5000
                )
                logging.info(f"Table {table_name} chargée avec succès.")
            
        logging.info("=== TOUTES LES DONNÉES ONT ÉTÉ CHARGÉES DANS LE DWH ===")
            
    except Exception as e:
        logging.error(f"!!! ERREUR LORS DU CHARGEMENT : {e}")
        raise
    finally:
        engine.dispose()