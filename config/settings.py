import os

# Paramètres de connexion PostgreSQL
DB_USER = "postgres"
DB_PASS = "Najehwac1234"
DB_HOST = "127.0.0.1" 
DB_PORT = "5432" # Changé de 1432 à 5432
DB_NAME = "mexora_dwh"
DB_SCHEMA = "dwh_mexora"

# URL de connexion SQLAlchemy
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Chemins des fichiers de données
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

FILES = {
    "commandes": os.path.join(DATA_DIR, "commandes_mexora.csv"),
    "clients": os.path.join(DATA_DIR, "clients_mexora.csv"),
    "produits": os.path.join(DATA_DIR, "produits_mexora.json"),
    "regions": os.path.join(DATA_DIR, "regions_maroc.csv"),
}
