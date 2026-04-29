-- 1. Vérifier si des ventes n'ont pas de clients correspondants (Orphelins)
SELECT count(*) as ventes_sans_client
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_client c ON f.id_client = c.id_client_sk
WHERE c.id_client_sk IS NULL;

-- 2. Vérifier si des ventes n'ont pas de produits correspondants
SELECT count(*) as ventes_sans_produit
FROM dwh_mexora.fait_ventes f
LEFT JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE p.id_produit_sk IS NULL;

-- 3. Vérifier le nombre de lignes par table
SELECT 'dim_client' as table, count(*) FROM dwh_mexora.dim_client
UNION ALL
SELECT 'dim_produit', count(*) FROM dwh_mexora.dim_produit
UNION ALL
SELECT 'fait_ventes', count(*) FROM dwh_mexora.fait_ventes;

-- 4. Vérifier les doublons de clés naturelles dans les dimensions
SELECT id_client_nk, count(*) 
FROM dwh_mexora.dim_client 
WHERE est_actif = TRUE 
GROUP BY id_client_nk HAVING count(*) > 1;