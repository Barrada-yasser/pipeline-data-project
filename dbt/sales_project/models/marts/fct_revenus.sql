SELECT
    c.client_id,
    cl.nom AS nom_client,
    cl.ville,
    SUM(p.prix * c.quantite) AS chiffre_affaires
FROM {{ ref('stg_commandes') }} c
JOIN raw_produits p ON c.produit_id = p.id
JOIN raw_clients cl ON c.client_id = cl.id
GROUP BY c.client_id, cl.nom, cl.ville
ORDER BY chiffre_affaires DESC