# 📊 Rapport de Transformations — Mexora Analytics

Ce document récapitule l'ensemble des règles métier appliquées lors de la phase de transformation (**Transform**) du pipeline ETL, conformément à l’énoncé. Chaque règle est documentée avec son impact (nombre de lignes affectées) issu des logs.

---

## 1. Transformations des Commandes (`commandes_mexora.csv`)

| Règle | Description Métier | Logique / Code | Lignes affectées |
|-------|-------------------|----------------|------------------|
| **R1** | Suppression des doublons sur `id_commande` (conserver la dernière) | `df.drop_duplicates(subset=['id_commande'], keep='last')` | ~1 545 |
| **R2** | Standardisation des dates (format YYYY-MM-DD) | `pd.to_datetime(errors='coerce', format='mixed')` + suppression des `NaT` | ~300 dates invalides |
| **R3** | Harmonisation des villes **via le référentiel** `regions_maroc.csv` | Chargement du CSV, création d’un dictionnaire `{variante: nom_standard}` et mapping | 100% des villes |
| **R4** | Standardisation des statuts (incluant `retourné`) | Mapping : `{livré, annulé, en_cours, retourné}` ; les non-reconnus → `inconnu` | Tous les statuts |
| **R5** | Suppression des lignes avec `quantite <= 0` | `df = df[df['quantite'] > 0]` | ~380 |
| **R6** | Suppression des lignes avec `prix_unitaire = 0` (commandes test) | `df = df[df['prix_unitaire'] > 0]` | ~260 |
| **R7** | Remplacement des `id_livreur` manquants par `-1` | `df['id_livreur'].fillna('-1')` | 7% des lignes |

> **Statistiques finales :**  
> - Extraites : **51 500**  
> - Validées : **49 015**  
> - Taux de rejet : **4,8 %**

---

## 2. Transformations des Clients (`clients_mexora.csv`)

| Règle | Description Métier | Logique / Code | Lignes affectées |
|-------|-------------------|----------------|------------------|
| **R1** | Déduplication sur email normalisé | `df.sort_values('date_inscription').drop_duplicates(subset=['email_norm'])` | ~47 doublons |
| **R2** | Standardisation du sexe (cible : `m` / `f` / `inconnu`) | Mapping dictionnaire | 100% des lignes |
| **R3** | Validation âge (entre 16 et 100 ans) ; calcul de `tranche_age` | `age = (today - date_naissance).days // 365` ; filtrage + catégorisation | ~33 exclus |
| **R4** | Validation du format email | Regex `^[a-zA-Z0-9._%+-]+@...` | ~17 emails invalides mis à `NULL` |
| **R5** | ~~Segmentation client~~ | **Déplacée dans `build_dim_client`** (calcul basé sur CA 12 mois, Gold/Silver/Bronze) | – |

> **Note :** La segmentation n’est plus effectuée dans le nettoyage pour respecter la séparation des responsabilités. Elle est calculée **une seule fois** lors de la construction de la dimension client.

> **Statistiques finales :**  
> - Extraits : **1 040**  
> - Validés : **943**

---

## 3. Transformations des Produits (`produits_mexora.json`)

| Règle | Description | Logique | Impact |
|-------|-------------|---------|--------|
| **R1** | Normalisation de la casse des catégories | `str.strip().str.capitalize()` | Uniformisation |
| **R2** | Gestion des prix `NULL` → 0 | `fillna(0)` | ~5 produits |
| **R3** | Initialisation des colonnes SCD Type 2 | `date_debut = today`, `date_fin = '9999-12-31'`, `est_actif = True` | Tous les produits |

---

## 4. Enrichissement & Modélisation (Star Schema)

- **Dimension temporelle** : génération automatique d’un calendrier (2020–2026) avec `periode_ramadan` et `est_ferie_maroc`.
- **Calcul des mesures** : `montant_ht = quantite * prix_unitaire`, `montant_ttc = montant_ht * 1,20`.
- **Surrogate keys** : remplacement des identifiants métiers (`id_client_nk`, etc.) par des clés auto-incrémentées.
- **Segmentation client** : calculée sur le CA des 12 derniers mois (règles Mexora : ≥15 000 MAD → Gold, ≥5 000 → Silver, sinon Bronze).

> Toutes ces transformations sont tracées dans les logs (`logs/etl_*.log`) avec le détail des lignes affectées par règle.