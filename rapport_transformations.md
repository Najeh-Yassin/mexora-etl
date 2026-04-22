# 📊 Rapport de Transformations — Mexora Analytics

Ce document récapitule l'ensemble des règles métier appliquées lors de la phase de transformation (**Transform**) du pipeline ETL. L'objectif est de garantir que les données chargées dans le Data Warehouse sont propres, cohérentes et exploitables pour l'analyse décisionnelle.

---

## 1. Transformations des Commandes (`commandes_mexora.csv`)

La table de faits repose sur l'intégrité de ces données. Nous avons traité les problèmes intentionnels de saisie et de formatage.

| Règle | Description Métier | Logique / Code Python | Impact (Lignes affectées) |
| :--- | :--- | :--- | :--- |
| **R1** | Suppression des doublons | `drop_duplicates(subset=['id_commande'], keep='last')` | ~1 545 doublons supprimés |
| **R2** | Standardisation des dates | `pd.to_datetime(errors='coerce')` + `dropna()` | ~300 dates invalides rejetées |
| **R3** | Harmonisation des villes | Mapping (ex: 'TNG', 'Tnja' -> 'Tanger') + `str.title()` | 100% des villes standardisées |
| **R4** | Standardisation statuts | Mapping vers {livré, annulé, en_cours, retourné} | Tous les statuts harmonisés |
| **R5** | Nettoyage quantités | Filtre strict : `df['quantite'] > 0` | ~380 erreurs de saisie exclues |
| **R6** | Nettoyage prix | Filtre strict : `df['prix_unitaire'] > 0` | ~260 commandes tests supprimées |
| **R7** | Livreurs manquants | Remplacement des valeurs `NaN` par `"INCONNU"` | 7% des lignes modifiées |

> **Statistiques Finales :** 
> - Lignes extraites : **51 500**  
> - Lignes validées : **49 015**  
> - Taux de rejet : **4.8%**

---

## 2. Transformations des Clients (`clients_mexora.csv`)

Le nettoyage des clients permet une segmentation précise et une analyse démographique fiable.

| Règle | Description Métier | Logique / Code Python | Impact (Lignes affectées) |
| :--- | :--- | :--- | :--- |
| **R1** | Déduplication Clients | `drop_duplicates(subset=['email'], keep='last')` | ~47 doublons supprimés |
| **R2** | Standardisation Sexe | Mapping {1: 'm', 0: 'f', 'homme': 'm', ...} | Tous les genres normalisés |
| **R3** | Validation Âge | Calcul `Année_Actuelle - Année_Naissance`. Filtre [16 - 100 ans] | ~33 clients hors-limites rejetés |
| **R4** | Validation Email | Regex : `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$` | ~17 emails mal formatés rejetés |
| **R5** | **Segmentation RFM** | CA Cumulé (Livré) : Gold (≥15k), Silver (≥5k), Bronze | 100% des clients segmentés |

> **Statistiques Finales :** 
> - Clients extraits : **1 040**  
> - Clients validés : **943**

---

## 3. Transformations des Produits (`produits_mexora.json`)

Préparation du catalogue produit pour la gestion de l'historique (SCD).

| Règle | Description Métier | Logique / Code Python | Impact |
| :--- | :--- | :--- | :--- |
| **R1** | Casse catégories | `str.strip().str.capitalize()` | Uniformisation des catégories |
| **R2** | Initialisation SCD 2 | Ajout des colonnes `date_debut`, `date_fin`, `est_actif` | Prêt pour l'historisation |
| **R3** | Mapping Prix | Renommage `prix_catalogue` en `prix_standard` | Alignement avec le schéma SQL |

---

## 4. Enrichissement & Modélisation (Star Schema)

Outre le nettoyage, le pipeline effectue des jointures complexes pour passer d'un modèle transactionnel à un modèle décisionnel :

1. **Mapping Surrogate Keys (SK) :** Remplacement des identifiants métiers (`id_client_nk`) par des clés techniques numériques dans la table de faits.
2. **Dimension Temporelle :** Génération automatique d'un calendrier incluant l'attribut `periode_ramadan` (basé sur les dates réelles 2023-2025).
3. **Calcul des Faits :** Calcul automatique du `montant_ht` et du `montant_ttc` (TVA 20%) pour chaque ligne de vente.

