**Mexora Analytics — Pipeline ETL & Data Warehouse**
Ce projet constitue le système décisionnel de Mexora, une marketplace e-commerce basée à Tanger. Il automatise l'extraction, le nettoyage et le chargement des données opérationnelles vers un entrepôt de données (Data Warehouse) sous PostgreSQL.

**📋 Présentation du Projet**
L'objectif est de transformer des données brutes imparfaites (doublons, formats de dates mixtes, erreurs de saisie) en un modèle dimensionnel (Schéma en étoile) permettant des analyses rapides sur :
L'évolution du Chiffre d'Affaires par région.
Le comportement des segments clients (Gold, Silver, Bronze).
L'impact des périodes spécifiques comme le Ramadan.

**🛠️ Stack Technique**
Langage : Python 3.11+
Manipulation de données : Pandas
Base de données : PostgreSQL 15+
ORM / Connexion : SQLAlchemy, Psycopg2
Versioning : Git & GitHub

**📂 Structure du Projet Python**
Le code est organisé de façon modulaire conformément aux standards de la Data Engineering :
code
Text
mexora_etl/
├── data/                # Fichiers sources (CSV, JSON)
│   ├── clients_mexora.csv
│   ├── commandes_mexora.csv   
│   ├── produits_mexora.json
│   └── regions_maroc.csv
├── logs/                # Rapports d'exécution générés
├── config/
│   └── settings.py      # Paramètres de connexion PostgreSQL
├── extract/
│   └── extractor.py     # Logique d'extraction multi-sources
├── transform/
│   ├── clean_commandes.py  # Nettoyage R1-R7 (dates, villes, prix)
│   ├── clean_clients.py    # Nettoyage R1-R5 (sexe, âge, segmentation)
│   ├── clean_produits.py   # Nettoyage R1-R3 (catégories, prix null)
│   └── build_dimensions.py # Création du temps (Ramadan) et SCD2
├── load/
│   └── loader.py        # Fonctions de chargement SQL
├── utils/
│   └── logger.py        # Configuration du logging
├── main.py              # Orchestrateur (Point d'entrée)
└── requirements.txt     # Dépendances du projet


**🚀 Installation et Utilisation**
1. **Prérequis**
Assurez-vous d'avoir Python et PostgreSQL installés sur votre machine.
2. **Installation**
Clonez le dépôt et installez les bibliothèques nécessaires :

git clone https://github.com/Najeh-Yassin/mexora-etl
cd mexora_etl
pip install -r requirements.txt

3. **Configuration**   
Modifiez le fichier config/settings.py avec vos identifiants PostgreSQL :

DB_USER = "votre_utilisateur"
DB_PASSWORD = "votre_password"
DB_NAME = "mexora_dwh"


4. **Lancement du Pipeline**
Exécutez la commande suivante pour lancer l'ETL :

python main.py

**📊 Transformations Appliquées**
Les transformations incluent :
Standardisation : Harmonisation des villes marocaines (TNG -> Tanger).
Validation : Filtrage des prix négatifs et des clients de moins de 16 ans.
Calcul métier : Segmentation automatique des clients basée sur le CA des 12 derniers mois.
Historisation : Préparation des dimensions pour le SCD Type 2.

ABAJJA MOHAMMED
Date : Avril 2026