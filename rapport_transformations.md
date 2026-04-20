# Rapport de Transformations — Mexora Analytics

## 1. Transformations des Commandes (commandes_mexora.csv)
| Règle | Description Métier | Code / Logique | Lignes affectées |
| :--- | :--- | :--- | :--- |
| **R1** | Suppression des doublons | `drop_duplicates(subset=['id_commande'])` | ... lignes suppr. |
| **R2** | Standardisation des dates | `pd.to_datetime(format='mixed')` | ... dates NaT suppr. |
| **R3** | Harmonisation des villes | Mapping via `regions_maroc.csv` | ... villes corrigées |
| **R4** | Standardisation statuts | Mapping vers {livré, annulé, en_cours, retourné} | ... statuts modifiés |
| **R5** | Quantités invalides | `df[df['quantite'] > 0]` | ... lignes suppr. |
| **R6** | Prix nuls (tests) | `df[df['prix_unitaire'] > 0]` | ... lignes suppr. |
| **R7** | Livreurs manquants | `fillna('-1')` | ... valeurs remplacées |

## 2. Transformations des Clients (clients_mexora.csv)
| Règle | Description Métier | Code / Logique | Lignes affectées |
| :--- | :--- | :--- | :--- |
| **R1** | Déduplication Email | `drop_duplicates(subset=['email_norm'])` | ... doublons suppr. |
| **R2** | Standardisation Sexe | Mapping (1/0/H/F) -> 'm' ou 'f' | ... sexes corrigés |
| **R3** | Validation Âge | Calcul (Now - Naissance). Filtre 16-100 ans | ... clients suppr. |
| **R4** | Format Email | Regex validation format standard | ... emails invalidés |
| **R5** | Segmentation | Calcul CA 12 mois (Gold > 15k, Silver > 5k) | 100% des clients |

## 3. Transformations des Produits (produits_mexora.json)
| Règle | Description Métier | Code / Logique | Lignes affectées |
| :--- | :--- | :--- | :--- |
| **R1** | Casse catégories | `str.capitalize()` | ... lignes modifiées |
| **R2** | SCD Type 2 | Ajout `date_debut`, `date_fin`, `est_actif` | Tous les produits |