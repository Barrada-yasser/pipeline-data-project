from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="pipeline_ventes",
    description="Pipeline de ventes : ingestion des données brutes",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    verifier_connexion = BashOperator(
        task_id="verifier_connexion",
        bash_command="echo 'Connexion à la base dev OK'"
    )

    ingestion = BashOperator(
        task_id="ingestion_csv",
        bash_command="cd /opt/airflow && pip install psycopg2-binary -q && python ingest.py",
        env={
            "DB_HOST": "postgres_dev",
            "DB_PORT": "5432"
        }
    )

    verifier_connexion >> ingestion