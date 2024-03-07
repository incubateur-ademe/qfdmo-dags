from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.utils import load_table, normalize_nom, normalize_url, normalize_phone_number, normalize_email, \
    find_differences, save_to_database


def read_data_from_postgres(**kwargs):
    table_name = kwargs["table_name"]
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
    df = load_table(table_name, engine)
    return df


def normalize_and_find_differences(**kwargs):
    ti = kwargs['ti']
    df_act = ti.xcom_pull(task_ids='load_imported_actors')
    df_rev_act = ti.xcom_pull(task_ids='load_imported_revision_actors')

    columns_to_exclude = ["identifiant_unique", "statut", "cree_le", "modifie_le"]
    normalization_map = {
        "nom": normalize_nom,
        "nom_commercial": normalize_nom,
        "ville": normalize_nom,
        "url": normalize_url,
        "adresse": normalize_nom,
        "adresse_complement": normalize_nom,
        "email": normalize_email,
        "telephone": normalize_phone_number,
    }

    df_differences = find_differences(df_act, df_rev_act, columns_to_exclude, normalization_map)

    return df_differences


def save_results_to_database(**kwargs):
    df_cleaned = kwargs['ti'].xcom_pull(task_ids='normalize_and_find_differences')
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
    save_to_database(df_cleaned, "qfdmo_correctionequipeacteur", engine)


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
    'manual_actor_updates',
    default_args=default_args,
    description='DAG for manually updated LVAO actors data',
    schedule_interval=None,
)

t1 = PythonOperator(
    task_id='load_imported_actors',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_acteur"},
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_imported_revision_actors',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_revisionacteur"},
    dag=dag,
)

t3 = PythonOperator(
    task_id='normalize_and_find_differences',
    python_callable=normalize_and_find_differences,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='save_results_to_database',
    python_callable=save_results_to_database,
    provide_context=True,
    dag=dag,
)

[t1, t2] >> t3 >> t4
























































































