import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Fichiers sources
COMMANDES_FILE = os.path.join(DATA_DIR, 'commandes_mexora.csv')
PRODUITS_FILE = os.path.join(DATA_DIR, 'produits_mexora.json')
CLIENTS_FILE = os.path.join(DATA_DIR, 'clients_mexora.csv')
REGIONS_FILE = os.path.join(DATA_DIR, 'regions_maroc.csv')

# Base de données PostgreSQL
# À modifier avec vos informations de connexion
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mexora_dwh")

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
