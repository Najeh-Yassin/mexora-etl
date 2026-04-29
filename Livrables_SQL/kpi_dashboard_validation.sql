-- KPI 1 : Évolution du CA mensuel (N vs N-1)
WITH ca_mensuel AS (
    SELECT annee, mois, SUM(ca_ttc) AS ca_total
    FROM reporting_mexora.mv_ca_mensuel
    GROUP BY annee, mois
)
SELECT annee, mois, ca_total,
       LAG(ca_total) OVER (ORDER BY annee, mois) AS ca_prec,
       ROUND(((ca_total - LAG(ca_total) OVER (ORDER BY annee, mois)) / 
       NULLIF(LAG(ca_total) OVER (ORDER BY annee, mois), 0)) * 100, 2) AS evolution_pct
FROM ca_mensuel
ORDER BY annee DESC, mois DESC LIMIT 5;

-- KPI 2 : Panier moyen par segment client
SELECT c.segment_client,
       COUNT(DISTINCT f.id_vente) AS nb_commandes,
       ROUND(SUM(f.montant_ttc) / COUNT(DISTINCT f.id_vente), 2) AS panier_moyen
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_client c ON f.id_client = c.id_client_sk
WHERE f.statut_commande = 'livré'
GROUP BY c.segment_client
ORDER BY panier_moyen DESC;

-- KPI 3 : Taux de retour par catégorie
SELECT p.categorie,
       COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') AS nb_retours,
       ROUND(COUNT(*) FILTER (WHERE f.statut_commande = 'retourné') * 100.0 / COUNT(*), 2) AS taux_retour_pct
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
GROUP BY p.categorie
ORDER BY taux_retour_pct DESC;

-- KPI 4 : Effet Ramadan sur l'alimentation
SELECT t.periode_ramadan,
       ROUND(AVG(ca_ttc), 2) AS ca_moyen_journalier,
       SUM(volume_vendu) AS volume_total
FROM reporting_mexora.mv_ca_mensuel
WHERE categorie = 'Alimentation'
GROUP BY t.periode_ramadan;