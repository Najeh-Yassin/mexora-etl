"""
config/settings.py
Paramètres de connexion et chemins du projet Mexora ETL
"""

# ── Base de données ───────────────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "mexora_dwh"
DB_USER     = "postgres"
DB_PASSWORD = "Najehwac1234"   # ← À modifier selon votre config

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Chemins des fichiers source ───────────────────────────────────────────────
DATA_DIR              = "data"
PATH_COMMANDES        = f"{DATA_DIR}/commandes_mexora.csv"
PATH_CLIENTS          = f"{DATA_DIR}/clients_mexora.csv"
PATH_PRODUITS         = f"{DATA_DIR}/produits_mexora.json"
PATH_REGIONS          = f"{DATA_DIR}/regions_maroc.csv"

# ── Dictionnaire FILES (utilisé par extractor.py) ─────────────────────────────
FILES = {
    "commandes": PATH_COMMANDES,
    "clients":   PATH_CLIENTS,
    "produits":  PATH_PRODUITS,
    "regions":   PATH_REGIONS,
}

# ── Dimension temporelle ──────────────────────────────────────────────────────
DIM_TEMPS_DATE_DEBUT  = "2020-01-01"
DIM_TEMPS_DATE_FIN    = "2026-12-31"

# ── Règles de segmentation client ─────────────────────────────────────────────
SEUIL_GOLD   = 15_000   # MAD
SEUIL_SILVER =  5_000   # MAD

# ── Schéma PostgreSQL cible ───────────────────────────────────────────────────
SCHEMA_DWH      = "dwh_mexora"
SCHEMA_STAGING  = "staging_mexora"
SCHEMA_REPORTING = "reporting_mexora"