import logging
from extract.extractor import extract_csv, extract_json
from transform.clean_commandes import transform_commandes
from transform.clean_clients import transform_clients
from transform.clean_produits import transform_produits
from transform.build_dimensions import build_dim_temps, add_scd2_columns, build_fait_ventes
from load.loader import get_engine, load_to_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(message)s')

def run_pipeline():
    # 1. EXTRACT
    df_cmd_raw = extract_csv('data/commandes_mexora.csv')
    df_clients_raw = extract_csv('data/clients_mexora.csv')
    df_prod_raw = extract_json('data/produits_mexora.json')
    df_reg_raw = extract_csv('data/regions_maroc.csv')

    # 2. TRANSFORM (Nettoyage)
    df_cmd = transform_commandes(df_cmd_raw, df_reg_raw)
    df_clients = transform_clients(df_clients_raw, df_cmd)
    df_prod = transform_produits(df_prod_raw)

    # 3. BUILD DIMENSIONS & SK
    dim_temps = build_dim_temps()
    
    # On ajoute les colonnes SCD2 et on simule les Surrogate Keys (SK)
    dim_client = add_scd2_columns(df_clients)
    dim_client['id_client_sk'] = range(1, len(dim_client) + 1)
    
    dim_produit = add_scd2_columns(df_prod)
    dim_produit.rename(columns={'id_produit': 'id_produit_nk'}, inplace=True)
    dim_produit['id_produit_sk'] = range(1, len(dim_produit) + 1)
    
    dim_region = df_reg_raw.copy()
    dim_region['id_region'] = range(1, len(dim_region) + 1)

    # 4. BUILD FAITS
    fait_ventes = build_fait_ventes(df_cmd, dim_client, dim_produit, dim_region)

    # 5. LOAD
    engine = get_engine('postgres', 'password', 'localhost', '5432', 'mexora_dwh')
    
    load_to_postgres(dim_temps, 'dim_temps', engine)
    load_to_postgres(dim_client, 'dim_client', engine)
    load_to_postgres(dim_produit, 'dim_produit', engine)
    load_to_postgres(dim_region, 'dim_region', engine)
    load_to_postgres(fait_ventes, 'fait_ventes', engine)
    
    logging.info("PIPELINE MEXORA ANALYTICS TERMINÉ !")

if __name__ == "__main__":
    run_pipeline()