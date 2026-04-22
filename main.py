import time
from utils.logger import setup_logger
from extract.extractor import extract_data
from transform.clean_commandes import clean_commandes
from transform.clean_clients import clean_clients
from transform.clean_produits import clean_produits
from transform.build_dimensions import build_dimensions
from load.loader import load_data

def main():
    logger = setup_logger()
    logger.info("=== DÉMARRAGE DU PIPELINE ETL MEXORA ANALYTICS ===")
    start_time = time.time()
    
    try:
        # 1. EXTRACTION
        raw_data = extract_data()
        
        # 2. TRANSFORMATION (NETTOYAGE)
        cleaned_data = {
            "commandes": clean_commandes(raw_data["commandes"]),
            "clients": clean_clients(raw_data["clients"]),
            "produits": clean_produits(raw_data["produits"]),
            "regions": raw_data["regions"] # Gardé tel quel pour le mapping
        }
        
        # 3. TRANSFORMATION (MODÉLISATION)
        star_schema = build_dimensions(cleaned_data)
        
        # 4. CHARGEMENT
        load_data(star_schema)
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f"=== PIPELINE TERMINÉ AVEC SUCCÈS EN {duration} SECONDES ===")
        
    except Exception as e:
        logger.error(f"!!! CRASH DU PIPELINE : {e}")
        raise

if __name__ == "__main__":
    main()