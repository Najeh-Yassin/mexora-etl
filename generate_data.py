import os
import json
import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# Initialisation de Faker avec la locale française
fake = Faker('fr_FR')

DATA_DIR = "data"

# ---------------------------------------------------------
# 4. Regions (Référentiel)
# ---------------------------------------------------------
def generate_regions():
    regions_data = [
        {"code_ville": "V01", "nom_ville_standard": "Tanger", "province": "Tanger-Assilah", "region_admin": "Tanger-Tétouan-Al Hoceima", "zone_geo": "Nord", "population": 1000000, "code_postal": "90000"},
        {"code_ville": "V02", "nom_ville_standard": "Casablanca", "province": "Casablanca", "region_admin": "Casablanca-Settat", "zone_geo": "Centre", "population": 3500000, "code_postal": "20000"},
        {"code_ville": "V03", "nom_ville_standard": "Rabat", "province": "Rabat", "region_admin": "Rabat-Salé-Kénitra", "zone_geo": "Centre", "population": 600000, "code_postal": "10000"},
        {"code_ville": "V04", "nom_ville_standard": "Fès", "province": "Fès", "region_admin": "Fès-Meknès", "zone_geo": "Centre", "population": 1100000, "code_postal": "30000"},
        {"code_ville": "V05", "nom_ville_standard": "Marrakech", "province": "Marrakech", "region_admin": "Marrakech-Safi", "zone_geo": "Sud", "population": 900000, "code_postal": "40000"},
        {"code_ville": "V06", "nom_ville_standard": "Agadir", "province": "Agadir-Ida-Ou Tanane", "region_admin": "Souss-Massa", "zone_geo": "Sud", "population": 500000, "code_postal": "80000"},
        {"code_ville": "V07", "nom_ville_standard": "Oujda", "province": "Oujda-Angad", "region_admin": "Oriental", "zone_geo": "Est", "population": 500000, "code_postal": "60000"},
        {"code_ville": "V08", "nom_ville_standard": "Meknès", "province": "Meknès", "region_admin": "Fès-Meknès", "zone_geo": "Centre", "population": 600000, "code_postal": "50000"},
        {"code_ville": "V09", "nom_ville_standard": "Kénitra", "province": "Kénitra", "region_admin": "Rabat-Salé-Kénitra", "zone_geo": "Centre", "population": 400000, "code_postal": "14000"},
        {"code_ville": "V10", "nom_ville_standard": "Tétouan", "province": "Tétouan", "region_admin": "Tanger-Tétouan-Al Hoceima", "zone_geo": "Nord", "population": 400000, "code_postal": "93000"},
    ]
    df = pd.DataFrame(regions_data)
    df.to_csv(os.path.join(DATA_DIR, "regions_maroc.csv"), index=False, encoding='utf-8')
    return [r["nom_ville_standard"] for r in regions_data]

# ---------------------------------------------------------
# 2. Produits
# ---------------------------------------------------------
def generate_produits():
    produits = []
    categories_base = ["Electronique", "Mode", "Alimentation", "Maison", "Sport"]
    
    noms_produits = ["iPhone 13", "Samsung Galaxy S21", "MacBook Pro", "TV Sony 55\"", "Casque Bose", 
                     "T-shirt Nike", "Jeans Levi's", "Baskets Adidas", "Robe Zara", "Veste Puma",
                     "Café Lavazza", "Chocolat Lindt", "Huile d'olive", "Pâtes Barilla", "Miel naturel",
                     "Canapé IKEA", "Lampe Philips", "Tapis persan", "Aspirateur Dyson", "Table basse",
                     "Vélo Trek", "Haltères 10kg", "Tapis de yoga", "Ballon de foot", "Raquette Wilson"]
    
    for i in range(1, 101):
        id_prod = f"P{i:03d}"
        nom = random.choice(noms_produits) + f" V{random.randint(1,5)}"
        cat_base = categories_base[random.randint(0, len(categories_base)-1)]
        
        # Anomalies categorie ~20%
        if random.random() < 0.20:
            cat = random.choice([cat_base.lower(), cat_base.upper(), cat_base])
        else:
            cat = cat_base
            
        # Anomalie prix_catalogue (null) ~3%
        if random.random() < 0.03:
            prix = None
        else:
            prix = round(random.uniform(50, 5000), 2)
            
        # Actif false ~5%
        actif = False if random.random() < 0.05 else True
        
        # Date de création entre 2020 et 2025
        d_creation = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2025, 12, 31)).strftime("%Y-%m-%d")
        
        produit = {
            "id_produit": id_prod,
            "nom": nom,
            "categorie": cat,
            "sous_categorie": "Standard",
            "marque": fake.company(),
            "fournisseur": fake.company(),
            "prix_catalogue": prix,
            "origine_pays": fake.country(),
            "date_creation": d_creation,
            "actif": actif
        }
        produits.append(produit)
        
    with open(os.path.join(DATA_DIR, "produits_mexora.json"), "w", encoding='utf-8') as f:
        json.dump({"produits": produits}, f, indent=4, ensure_ascii=False)
        
    return [p["id_produit"] for p in produits]

# ---------------------------------------------------------
# 3. Clients
# ---------------------------------------------------------
def generate_clients(villes_ref):
    clients = []
    canaux = ["Site web", "Application", "Publicité", "Parrainage"]
    
    for i in range(1, 1001):
        id_client = f"C_{i:04d}"
        
        # Sexe: codage varié
        sexe_rand = random.random()
        if sexe_rand < 0.33:
            sexe = random.choice(["m", "f"])
        elif sexe_rand < 0.66:
            sexe = random.choice(["1", "0"])
        else:
            sexe = random.choice(["Homme", "Femme"])
            
        # Anomalie date de naissance ~3%
        if random.random() < 0.03:
            d_naissance = random.choice(["2100-01-01", "1800-01-01", "2050-06-15"])
        else:
            d_naissance = fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d")
            
        # Incohérence ville de Tanger
        ville_base = random.choice(villes_ref)
        if ville_base == "Tanger":
            ville = random.choice(["tanger", "TNG", "TANGER", "Tnja", "Tanger"])
        else:
            ville = ville_base
            
        # Anomalie email ~5%
        email_base = fake.email()
        if random.random() < 0.05:
            if random.choice([True, False]):
                email = email_base.replace("@", "")
            else:
                email = email_base.split("@")[0] + "gmail.com"
        else:
            email = email_base
            
        telephone = f"06{random.randint(10000000, 99999999)}"
        d_inscription = fake.date_between_dates(date_start=datetime(2020, 1, 1), date_end=datetime(2025, 12, 31)).strftime("%Y-%m-%d")
        
        clients.append({
            "id_client": id_client,
            "nom": fake.last_name(),
            "prenom": fake.first_name(),
            "email": email,
            "date_naissance": d_naissance,
            "sexe": sexe,
            "ville": ville,
            "telephone": telephone,
            "date_inscription": d_inscription,
            "canal_acquisition": random.choice(canaux)
        })

    # Doublons sur email, id_client différent (~4%)
    num_duplicates = int(1000 * 0.04)
    duplicates = random.sample(clients, num_duplicates)
    
    current_max_id = 1000
    for c in duplicates:
        current_max_id += 1
        new_client = c.copy()
        new_client["id_client"] = f"C_{current_max_id:04d}"
        clients.append(new_client)
        
    df = pd.DataFrame(clients)
    df.to_csv(os.path.join(DATA_DIR, "clients_mexora.csv"), index=False, encoding='utf-8')
    return [c["id_client"] for c in clients]

# ---------------------------------------------------------
# 1. Commandes
# ---------------------------------------------------------
def generate_commandes(villes_ref, ids_produits, ids_clients):
    commandes = []
    
    modes_paiement = ["Carte", "Virement", "PayPal", "Espèces"]
    statuts_normaux = ["livré", "annulé", "en_cours", "retourné"]
    statuts_anormaux = ["OK", "KO", "DONE"]
    
    num_commandes_base = 50000
    
    for i in range(1, num_commandes_base + 1):
        id_cmd = f"CMD_{i:05d}"
        id_client = random.choice(ids_clients)
        id_produit = random.choice(ids_produits)
        
        # Format des dates varié
        dt_cmd = fake.date_between_dates(date_start=datetime(2022, 1, 1), date_end=datetime.now())
        dt_liv = dt_cmd + timedelta(days=random.randint(1, 14))
        
        date_format_choice = random.randint(1, 3)
        if date_format_choice == 1:
            date_cmd_str = dt_cmd.strftime("%d/%m/%Y")
            date_liv_str = dt_liv.strftime("%d/%m/%Y")
        elif date_format_choice == 2:
            date_cmd_str = dt_cmd.strftime("%Y-%m-%d")
            date_liv_str = dt_liv.strftime("%Y-%m-%d")
        else:
            # Format: Nov 15 2024
            date_cmd_str = dt_cmd.strftime("%b %d %Y")
            date_liv_str = dt_liv.strftime("%b %d %Y")
            
        # Anomalie quantité negative ~1%
        if random.random() < 0.01:
            quantite = random.randint(-5, -1)
        else:
            quantite = random.randint(1, 10)
            
        # Anomalie prix_unitaire = 0 ~1%
        if random.random() < 0.01:
            prix_u = 0.0
        else:
            prix_u = round(random.uniform(10, 1000), 2)
            
        # Anomalie statut ~10%
        if random.random() < 0.10:
            statut = random.choice(statuts_anormaux)
        else:
            statut = random.choice(statuts_normaux)
            
        # Incohérence ville de livraison
        ville_base = random.choice(villes_ref)
        if ville_base == "Tanger":
            ville_liv = random.choice(["tanger", "TNG", "TANGER", "Tnja", "Tanger"])
        else:
            ville_liv = ville_base
            
        # Anomalie id_livreur manquant ~7%
        if random.random() < 0.07:
            id_livreur = None
        else:
            id_livreur = f"LIV_{random.randint(1, 200):03d}"
            
        cmd = {
            "id_commande": id_cmd,
            "id_client": id_client,
            "id_produit": id_produit,
            "date_commande": date_cmd_str,
            "quantite": quantite,
            "prix_unitaire": prix_u,
            "statut": statut,
            "ville_livraison": ville_liv,
            "mode_paiement": random.choice(modes_paiement),
            "id_livreur": id_livreur,
            "date_livraison": date_liv_str
        }
        commandes.append(cmd)

    # Doublons commandes ~3%
    num_duplicates_cmd = int(50000 * 0.03)
    duplicates_cmd = random.sample(commandes, num_duplicates_cmd)
    
    for c in duplicates_cmd:
        new_cmd = c.copy()
        # Légère modification pour distinguer le doublon de l'original tout en gardant le même id_commande
        new_cmd["quantite"] = new_cmd["quantite"] + random.choice([1, -1]) if new_cmd["quantite"] > 1 else 2
        commandes.append(new_cmd)
        
    df = pd.DataFrame(commandes)
    df.to_csv(os.path.join(DATA_DIR, "commandes_mexora.csv"), index=False, encoding='utf-8')
    
    return len(commandes)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("Génération des données en cours...")
    
    villes_ref = generate_regions()
    print(f"- regions_maroc.csv : {len(villes_ref)} lignes")
    
    ids_produits = generate_produits()
    print(f"- produits_mexora.json : {len(ids_produits)} produits")
    
    ids_clients = generate_clients(villes_ref)
    print(f"- clients_mexora.csv : {len(ids_clients)} lignes")
    
    total_cmd = generate_commandes(villes_ref, ids_produits, ids_clients)
    print(f"- commandes_mexora.csv : {total_cmd} lignes")
    
    print(f"\nGénération terminée avec succès ! Les 4 fichiers ont été sauvegardés dans le dossier '{DATA_DIR}/'.")

if __name__ == "__main__":
    main()
