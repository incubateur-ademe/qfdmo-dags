from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys

from utils.utils import load_table, normalize_nom, normalize_url, normalize_email, normalize_phone_number, \
    apply_normalization, save_to_database


def read_data_from_postgres():
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
    df = load_table("qfdmo_acteur", engine)
    return df


def apply_address_normalization(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='read_imported_actors')
   # normalization_map = {"address": normalize_nom, "adresse_complement": normalize_nom}
    #df_normalized = apply_normalization(df, normalization_map)
    return df


def apply_other_normalizations(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='BAN_normalization')
    columns_to_exclude = ["identifiant_unique", "statut", "cree_le", "modifie_le"]
  #  normalization_map = {
   #     "nom": normalize_nom,
   #     "nom_commercial": normalize_nom,
    #    "ville": normalize_nom,
   #     "url": normalize_url,
   #     "email": normalize_email,
  #      "telephone": normalize_phone_number,
  #  }
  #  df_cleaned = apply_normalization(df, normalization_map)
    return df


def write_data_to_postgres(**kwargs):
    df_cleaned = kwargs['ti'].xcom_pull(task_ids='other_normalizations')
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
    save_to_database(df_cleaned, "qfdmo_actors_processed", engine)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'imported_actors_preprocessing',
    default_args=default_args,
    description='DAG for normalizing and saving LVAO actors',
    schedule_interval=None,
)

t1 = PythonOperator(
    task_id='read_imported_actors',
    python_callable=read_data_from_postgres,
    dag=dag,
)

t2 = PythonOperator(
    task_id='BAN_normalization',
    python_callable=apply_address_normalization,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='other_normalizations',
    python_callable=apply_other_normalizations,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='write_data_to_postgres',
    python_callable=write_data_to_postgres,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3 >> t4
