import logging
import os
from datetime import datetime

def setup_logger():
    """
    Configure le logger pour le projet et retourne l'instance.
    """
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    log_filename = f"logs/etl_{datetime.now().strftime('%Y%m%d')}.log"
    
    # Configuration du logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() # Affiche aussi dans le terminal
        ]
    )
    
    # Retourne le logger de l'application
    return logging.getLogger("mexora_etl")
