SELECT
    id,
    client_id,
    produit_id,
    quantite,
    date_commande,
    statut
FROM raw_commandes
WHERE statut != 'annulé'