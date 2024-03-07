from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData
import pandas as pd
from utils.utils import load_table, save_to_database


def read_data_from_postgres(**kwargs):
    table_name = kwargs["table_name"]
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
    df = load_table(table_name, engine)
    return df


def apply_corrections(**kwargs):
    df_normalized_actors = kwargs['ti'].xcom_pull(task_ids='load_normalized_actors')
    df_manual_actor_updates = kwargs['ti'].xcom_pull(task_ids='load_manual_actor_updates')

    df_normalized_actors = df_normalized_actors.set_index('identifiant_unique')
    df_manual_actor_updates = df_manual_actor_updates.set_index('identifiant_unique')

    df_normalized_actors.update(df_manual_actor_updates)

    return df_normalized_actors.reset_index()

def apply_corrections_ps(**kwargs):
    df_propositionservice = kwargs['ti'].xcom_pull(task_ids='read_imported_propositionservice')
    df_manual_propositionservice_updates = kwargs['ti'].xcom_pull(task_ids='load_manual_propositionservice_updates')

    new_df = pd.concat([df_propositionservice, df_manual_propositionservice_updates]).drop_duplicates(
        subset=['acteur_service_id', 'action_id', 'acteur_id'])
    new_df['acteur_id'] = new_df['acteur_id'].fillna(new_df['revision_acteur_id'])
    new_df.drop(columns=['revision_acteur_id'], inplace=True)

    return new_df



def write_data_to_postgres(**kwargs):
    df_normalized_corrected_actors = kwargs['ti'].xcom_pull(task_ids='apply_corrections')
    df_proposition_services = kwargs['ti'].xcom_pull(task_ids='apply_corrections_ps')
    pg_hook = PostgresHook(postgres_conn_id='lvao-preprod')
    engine = pg_hook.get_sqlalchemy_engine()
   
    original_table_name_actor = 'qfdmo_displayedacteur'
    temp_table_name_actor = 'qfdmo_displayedacteurtemp'

    original_table_name_ps = 'qfdmo_displayedpropositionservice'
    temp_table_name_ps = 'qfdmo_displayedpropositionservicetemp'


    with engine.connect() as conn:
        conn.execute(f'DELETE FROM {temp_table_name_ps}')
        conn.execute(f'DELETE FROM {temp_table_name_actor}')


        df_normalized_corrected_actors[['identifiant_unique', 'nom', 'adresse', 'adresse_complement',
       'code_postal', 'ville', 'url', 'email', 'location', 'telephone',
       'multi_base', 'nom_commercial', 'nom_officiel', 'manuel',
       'label_reparacteur', 'siret', 'identifiant_externe', 'acteur_type_id',
       'statut', 'source_id', 'cree_le', 'modifie_le', 'naf_principal',
       'commentaires', 'horaires', 'description']].to_sql(temp_table_name_actor, engine, if_exists='append', index=False,
                                              method='multi', chunksize=1000)


        df_proposition_services[['id', 'acteur_service_id', 'action_id', 'acteur_id']].to_sql(temp_table_name_ps, engine, if_exists='append', index=False, method='multi',
                                      chunksize=1000)

        conn.execute(f'ALTER TABLE {original_table_name_actor} RENAME TO {original_table_name_actor}_old')
        conn.execute(f'ALTER TABLE {temp_table_name_actor} RENAME TO {original_table_name_actor}')
        conn.execute(f'ALTER TABLE {original_table_name_actor}_old RENAME TO {temp_table_name_actor}')

        conn.execute(f'ALTER TABLE {original_table_name_ps} RENAME TO {original_table_name_ps}_old')
        conn.execute(f'ALTER TABLE {temp_table_name_ps} RENAME TO {original_table_name_ps}')
        conn.execute(f'ALTER TABLE {original_table_name_ps}_old RENAME TO {temp_table_name_ps}')

    print("Table swap completed successfully.")


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
    'create_displayed_actors_and_propositionservice',
    default_args=default_args,
    description='DAG for applying correction on normalized actors and propositionservice',
    schedule_interval=None,
)

t1 = PythonOperator(
    task_id='load_normalized_actors',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_actors_processed"},
    dag=dag,
)

t1_bis = PythonOperator(
    task_id='read_imported_propositionservice',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_propositionservice"},
    dag=dag,
)


t2 = PythonOperator(
    task_id='load_manual_actor_updates',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_manual_actors_updates"},
    dag=dag,
)

t2_bis = PythonOperator(
    task_id='load_manual_propositionservice_updates',
    python_callable=read_data_from_postgres,
    op_kwargs={"table_name": "qfdmo_manual_propositionservice_updates"},
    dag=dag,
)


t3 = PythonOperator(
    task_id='apply_corrections',
    python_callable=apply_corrections,
    provide_context=True,
    dag=dag,
)

t3_bis = PythonOperator(
    task_id='apply_corrections_ps',
    python_callable=apply_corrections_ps,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='write_data_to_postgres',
    python_callable=write_data_to_postgres,
    provide_context=True,
    dag=dag,
)

[t1, t2 ] >> t3
[t1_bis, t2_bis] >> t3_bis
[t3, t3_bis] >> t4

