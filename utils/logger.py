"""
utils/logger.py
Configuration du système de logging pour le pipeline ETL Mexora.
"""

import logging
import os
from datetime import datetime


def setup_logger(name: str = "mexora_etl", log_dir: str = "logs") -> logging.Logger:
    """
    Configure et retourne un logger nommé.
    - Écrit dans la console (stdout)
    - Écrit dans un fichier logs/etl_YYYYMMDD_HHMMSS.log

    IMPORTANT : retourne toujours un logging.Logger valide (jamais None).
    """
    # Créer le dossier logs s'il n'existe pas
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # Évite d'ajouter plusieurs handlers si setup_logger() est appelé plusieurs fois
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler fichier
    log_filename = os.path.join(
        log_dir,
        f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    file_handler = logging.FileHandler(log_filename, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logger initialisé — fichier : {log_filename}")
    return logger