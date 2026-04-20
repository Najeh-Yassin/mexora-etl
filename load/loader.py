import pandas as pd
import logging
from sqlalchemy import create_engine

def get_engine(user, password, host, port, db):
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def load_to_postgres(df, table_name, engine, schema='dwh_mexora'):
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace', # On remplace pour les dimensions
            index=False,
            method='multi',
            chunksize=1000
        )
        logging.info(f"[LOAD] {table_name} : {len(df)} lignes chargées.")
    except Exception as e:
        logging.error(f"Erreur lors du chargement de {table_name}: {e}")