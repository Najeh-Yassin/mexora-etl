-- ==========================================================
-- 1. CRÉATION DES SCHÉMAS
-- ==========================================================
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;

-- ==========================================================
-- 2. CRÉATION DES TABLES DE DIMENSIONS
-- ==========================================================

-- Dimension Temps
CREATE TABLE dwh_mexora.dim_temps (
    id_date           INTEGER PRIMARY KEY,
    jour              SMALLINT NOT NULL,
    mois              SMALLINT NOT NULL,
    trimestre         SMALLINT NOT NULL,
    annee             SMALLINT NOT NULL,
    semaine           SMALLINT,
    libelle_jour      VARCHAR(20),
    libelle_mois      VARCHAR(20),
    est_weekend       BOOLEAN DEFAULT FALSE,
    est_ferie_maroc   BOOLEAN DEFAULT FALSE,
    periode_ramadan   BOOLEAN DEFAULT FALSE
);

-- Dimension Produit (SCD Type 2)
CREATE TABLE dwh_mexora.dim_produit (
    id_produit_sk     SERIAL PRIMARY KEY,
    id_produit_nk     VARCHAR(20) NOT NULL,
    nom_produit       VARCHAR(200) NOT NULL,
    categorie         VARCHAR(100),
    sous_categorie    VARCHAR(100),
    marque            VARCHAR(100),
    fournisseur       VARCHAR(100),
    prix_standard     DECIMAL(10,2),
    origine_pays      VARCHAR(50),
    date_debut        DATE NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension Client
CREATE TABLE dwh_mexora.dim_client (
    id_client_sk      SERIAL PRIMARY KEY,
    id_client_nk      VARCHAR(20) NOT NULL,
    nom_complet       VARCHAR(200),
    tranche_age       VARCHAR(10),
    sexe              CHAR(1),
    ville             VARCHAR(100),
    region_admin      VARCHAR(100),
    segment_client    VARCHAR(20),
    canal_acquisition VARCHAR(50),
    date_debut        DATE NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dimension Région
CREATE TABLE dwh_mexora.dim_region (
    id_region         SERIAL PRIMARY KEY,
    ville             VARCHAR(100) NOT NULL,
    province          VARCHAR(100),
    region_admin      VARCHAR(100),
    zone_geo          VARCHAR(50),
    pays              VARCHAR(50) DEFAULT 'Maroc'
);

-- Dimension Livreur
CREATE TABLE dwh_mexora.dim_livreur (
    id_livreur        SERIAL PRIMARY KEY,
    id_livreur_nk     VARCHAR(20),
    nom_livreur       VARCHAR(100),
    type_transport    VARCHAR(50),
    zone_couverture   VARCHAR(100)
);

-- ==========================================================
-- 3. CRÉATION DE LA TABLE DE FAITS
-- ==========================================================
CREATE TABLE dwh_mexora.fait_ventes (
    id_vente              BIGSERIAL PRIMARY KEY,
    id_date               INTEGER NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit            INTEGER NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client             INTEGER NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region             INTEGER NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur            INTEGER REFERENCES dwh_mexora.dim_livreur(id_livreur),
    quantite_vendue       INTEGER NOT NULL,
    montant_ht            DECIMAL(12,2) NOT NULL,
    montant_ttc           DECIMAL(12,2) NOT NULL,
    cout_livraison        DECIMAL(8,2),
    delai_livraison_jours SMALLINT,
    remise_pct            DECIMAL(5,2) DEFAULT 0,
    date_chargement       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    statut_commande       VARCHAR(20)
);

-- ==========================================================
-- 4. INDEXATION POUR LA PERFORMANCE
-- ==========================================================
CREATE INDEX idx_fv_date    ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX idx_fv_produit ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX idx_fv_client  ON dwh_mexora.fait_ventes(id_client);

-- ==========================================================
-- 5. VUES MATÉRIALISÉES (REPORTING)
-- ==========================================================

-- Vue 1 : CA Mensuel
CREATE MATERIALIZED VIEW reporting_mexora.mv_ca_mensuel AS
SELECT t.annee, t.mois, t.libelle_mois, t.periode_ramadan, r.region_admin, p.categorie,
       SUM(f.montant_ttc) AS ca_ttc, SUM(f.quantite_vendue) AS volume_vendu
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
JOIN dwh_mexora.dim_region r ON f.id_region = r.id_region
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE f.statut_commande = 'livré'
GROUP BY 1,2,3,4,5,6 WITH DATA;

-- Vue 2 : Top Produits
CREATE MATERIALIZED VIEW reporting_mexora.mv_top_produits AS
SELECT t.annee, t.trimestre, p.nom_produit, SUM(f.montant_ttc) AS ca_total
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps t ON f.id_date = t.id_date
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
GROUP BY 1,2,3 WITH DATA;