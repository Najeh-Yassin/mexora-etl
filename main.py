import time
import logging
from utils.logger import setup_logger
from extract.extractor import extract_data
from transform.clean_commandes import clean_commandes
from transform.clean_clients import clean_clients
from transform.clean_produits import clean_produits
from transform.build_dimensions import build_dimensions
from load.loader import load_data

def main():
    # Initialisation du logger
    logger = setup_logger()
    logger.info("=== DÉMARRAGE DU PIPELINE ETL MEXORA ANALYTICS ===")
    start_time = time.time()
    
    try:
        # 1. EXTRACTION
        logger.info("Phase 1 : Extraction des données...")
        raw_data = extract_data()
        
        # 2. TRANSFORMATION (NETTOYAGE)
        logger.info("Phase 2 : Nettoyage et Segmentation...")
        # On nettoie les commandes en premier
        cleaned_cmds = clean_commandes(raw_data["commandes"])
        
        cleaned_data = {
            "commandes": cleaned_cmds,
            # On passe cleaned_cmds ici pour le CA des 12 derniers mois
            "clients": clean_clients(raw_data["clients"], cleaned_cmds), 
            "produits": clean_produits(raw_data["produits"]),
            "regions": raw_data["regions"] 
        }
        
        # 3. MODÉLISATION (SCHÉMA EN ÉTOILE)
        logger.info("Phase 3 : Construction du schéma en étoile...")
        star_schema = build_dimensions(cleaned_data)
        
        # 4. CHARGEMENT
        logger.info("Phase 4 : Chargement vers PostgreSQL...")
        load_data(star_schema)
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f"=== PIPELINE TERMINÉ AVEC SUCCÈS EN {duration} SECONDES ===")
        
    except Exception as e:
        logger.error(f"!!! CRASH DU PIPELINE : {e}")
        # On affiche l'erreur complète dans la console pour vous aider
        import traceback
        traceback.print_exc()

# --- TRÈS IMPORTANT : C'EST CETTE LIGNE QUI LANCE TOUT ---
if __name__ == "__main__":
    main()