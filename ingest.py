import psycopg2
import csv
import os

host = os.getenv("DB_HOST", "localhost")
port = int(os.getenv("DB_PORT", "5433"))

conn = psycopg2.connect(
    host=host,
    port=port,
    database="dev_db",
    user="admin",
    password="admin",
    options="-c client_encoding=UTF8"
)
cursor = conn.cursor()
# Vide les tables avant de recharger
cursor.execute("TRUNCATE TABLE raw_clients, raw_produits, raw_commandes;")
print("🗑️ Tables vidées")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_clients (
        id INTEGER, nom TEXT, email TEXT, ville TEXT
    );
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_produits (
        id INTEGER, nom TEXT, categorie TEXT, prix NUMERIC
    );
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_commandes (
        id INTEGER, client_id INTEGER, produit_id INTEGER,
        quantite INTEGER, date_commande DATE, statut TEXT
    );
""")

def charger_csv(fichier, table, cursor):
    with open(fichier, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            valeurs = list(row.values())
            placeholders = ','.join(['%s'] * len(valeurs))
            cursor.execute(f"INSERT INTO {table} VALUES ({placeholders})", valeurs)
    print(f"✅ {table} chargée")

charger_csv('data/clients.csv',   'raw_clients',   cursor)
charger_csv('data/produits.csv',  'raw_produits',  cursor)
charger_csv('data/commandes.csv', 'raw_commandes', cursor)

conn.commit()
cursor.close()
conn.close()
print("✅ Ingestion terminée !")