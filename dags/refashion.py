import json
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.api_utils import fetch_dataset_from_point_apport
from utils.mapping_utils import transform_acteur_type_id, generate_unique_id
from utils.utils import transform_ecoorganisme, transform_location, extract_details

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'refashion',
    default_args=default_args,
    description='A pipeline to fetch, process, and load to validate data into postgresql for Refashion dataset',
    schedule_interval=None,
)


def fetch_data_from_api(**kwargs):
    dataset = kwargs["dataset"]
    api_url = f"https://data.pointsapport.ademe.fr/data-fair/api/v1/datasets/{dataset}/lines?size=10000"
    data =  fetch_dataset_from_point_apport(api_url)
    df = pd.DataFrame(data)
    return df

def create_proposition_services(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='create_actors')['df']
    data_dict = kwargs["ti"].xcom_pull(task_ids="load_data_from_postgresql")
    idx_max = data_dict['max_pds_idx']

    rows_list = []

    for index, row in df.iterrows():
        acteur_id = row['identifiant_unique']
        sous_categories = row['produitsdechets_acceptes']
        if row['point_dapport_de_service_reparation']:
            acteur_service_id = 17
            action_id = 1
        elif row['point_dapport_pour_reemploi']:
            acteur_service_id = 4
            action_id = 4
        elif row['point_de_reparation']:
            acteur_service_id = 15
            action_id = 1
        elif row['point_de_collecte_ou_de_reprise_des_dechets']:
            acteur_service_id = 4
            action_id = 11
        else:
            continue

        rows_list.append({"acteur_service_id": acteur_service_id, "action_id": action_id, "acteur_id": acteur_id,
                          "sous_categories": sous_categories})

    df_pds = pd.DataFrame(rows_list)
    df_pds.index = range(idx_max, idx_max + len(df_pds))
    df_pds['id'] = df_pds.index
    return df_pds


def create_proposition_services_sous_categories(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='create_proposition_services')

    rows_list = []
    sous_categories = {
        "Vêtement": 107,
        "Linge": 104,
        "Chaussure": 109
    }
    for index, row in df.iterrows():
        products = str(row["sous_categories"]).split("|")
        for product in products:
            if product.strip() in sous_categories:
                rows_list.append({
                    'propositionservice_id': row['id'],
                    'souscategorieobjet_id': sous_categories[product.strip()]
                })

    df_sous_categories = pd.DataFrame(rows_list, columns=['propositionservice_id', 'souscategorieobjet_id'])
    return df_sous_categories


def serialize_to_json(**kwargs):
    df_actors = kwargs["ti"].xcom_pull(task_ids="create_actors")['df']
    df_pds = kwargs["ti"].xcom_pull(task_ids="create_proposition_services")
    df_pdsc = kwargs["ti"].xcom_pull(task_ids="create_proposition_services_sous_categories")
    aggregated_pdsc = df_pdsc.groupby('propositionservice_id').apply(
        lambda x: x.to_dict('records')).reset_index(name='pds_sous_categories')

    df_pds_joined = pd.merge(df_pds, aggregated_pdsc, how='left', left_on='id', right_on='propositionservice_id')
    df_pds_joined.drop('id', axis=1, inplace=True)
    aggregated_pds = df_pds_joined.groupby('acteur_id').apply(lambda x: x.to_dict('records')).reset_index(
        name='proposition_services')

    df_joined = pd.merge(df_actors, aggregated_pds, how='left', left_on='identifiant_unique', right_on='acteur_id')

    df_joined.drop('acteur_id', axis=1, inplace=True)
    df_joined = df_joined.where(pd.notnull(df_joined), None)
    df_joined['row_updates'] = df_joined[[
                "identifiant_unique",
                "nom",
                "adresse",
                "adresse_complement",
                "code_postal",
                "ville",
                "url",
                "email",
                "location",
                "telephone",
                "nom_commercial",
                "label_reparacteur",
                "siret",
                "identifiant_externe",
                "acteur_type_id",
                "statut",
                "source_id",
                "cree_le",
                "modifie_le",
                "commentaires",
                "horaires",
                "proposition_services"
            ]].apply(lambda row: json.dumps(row.to_dict()), axis=1)

    return df_joined


def load_data_from_postgresql(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="lvao-preprod")
    engine = pg_hook.get_sqlalchemy_engine()

    df_acteurtype = pd.read_sql_table('qfdmo_acteurtype', engine)
    df_sources = pd.read_sql_table('qfdmo_source', engine)
    df_ps = pd.read_sql_table('qfdmo_propositionservice', engine)

    return {"acteurtype": df_acteurtype, "sources": df_sources, "max_pds_idx": df_ps['id'].max()}


def write_to_dagruns(**kwargs):
    dag_id = kwargs['dag'].dag_id
    run_id = kwargs['run_id']
    df = kwargs["ti"].xcom_pull(task_ids="serialize_actors_to_records")
    metadata = kwargs["ti"].xcom_pull(task_ids="create_actors")['metadata']
    pg_hook = PostgresHook(postgres_conn_id="lvao-preprod")
    engine = pg_hook.get_sqlalchemy_engine()
    current_date = datetime.now()
    with engine.connect() as conn:
        result = conn.execute("""
            INSERT INTO qfdmo_dagrun (dag_id, run_id, status, meta_data, created_date, updated_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING ID;
        """, (
            dag_id,
            1,
            'TO_VALIDATE',
            json.dumps(metadata),
            current_date,
            current_date
        ))
        dag_run_id = result.fetchone()[0]

        df['change_type'] = 'CREATE'
        df['dag_run_id'] =  dag_run_id
        df[['row_updates','dag_run_id','change_type']].to_sql(
            "qfdmo_dagrunchange",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )



def create_actors(**kwargs):
    data_dict = kwargs["ti"].xcom_pull(task_ids="load_data_from_postgresql")
    df = kwargs["ti"].xcom_pull(task_ids="fetch_data_from_api")
    df_sources = data_dict['sources']
    df_acteurtype = data_dict['acteurtype']


    column_mapping = {
        'id_point_apport_ou_reparation': 'identifiant_externe',
        'adresse_complement': 'adresse_complement',
        'type_de_point_de_collecte': 'acteur_type_id',
        'telephone': 'telephone',
        'siret': 'siret',
        'uniquement_sur_rdv': '',
        'exclusivite_de_reprisereparation': '',
        'filiere': '',
        'public_accueilli': '',
        'produitsdechets_acceptes': '',
        'labels_etou_bonus': '',
        'reprise': '',
        'point_de_reparation': '',
        'ecoorganisme': 'source_id',
        'adresse_format_ban': 'adresse',
        'nom_de_lorganisme': 'nom',
        'enseigne_commerciale': 'nom_commercial',
        '_updatedAt': 'cree_le',
        'site_web': 'url',
        'email': 'email',
        'perimetre_dintervention': '',
        'longitudewgs84': 'location',
        'latitudewgs84': 'location',
        'horaires_douverture': 'horaires',
        'consignes_dacces': 'commentaires',
    }

    selected_columns = ['nom', 'adresse', 'type_de_point_de_collecte', 'id_point_apport_ou_reparation']

    for old_col, new_col in column_mapping.items():
        if new_col:
            if old_col == 'type_de_point_de_collecte':
                df[new_col] = df[old_col].apply(lambda x: transform_acteur_type_id(x, df_acteurtype=df_acteurtype))
            elif old_col in ('longitudewgs84', 'latitudewgs84'):
                df['location'] = df.apply(lambda row: transform_location(row['longitudewgs84'], row['latitudewgs84']),
                                          axis=1)
            elif old_col == 'ecoorganisme':
                df[new_col] = df[old_col].apply(lambda x: transform_ecoorganisme(x, df_sources=df_sources))
            elif old_col == 'adresse_format_ban':
                df[['adresse', 'code_postal', 'ville']] = df.apply(extract_details, axis=1)
            else:
                df[new_col] = df[old_col]
    df['label_reparacteur'] = False
    df['identifiant_unique'] = df.apply(lambda x: generate_unique_id(x, selected_columns=selected_columns), axis=1)
    df['statut'] = 'ACTIF'
    df['modifie_le'] = df['cree_le']
    df['siret'] = df['siret'].astype(str).apply(lambda x: x[:14])
    df['telephone'] = df['telephone'].dropna().apply(lambda x: x.replace(' ', ''))
    df['telephone'] = df['telephone'].dropna().apply(lambda x: '0' + x[2:] if x.startswith('33') else x)

    duplicates_mask = df.duplicated('identifiant_unique', keep=False)
    duplicate_ids = df.loc[duplicates_mask, 'identifiant_unique'].unique()

    number_of_duplicates = len(duplicate_ids)

    df.drop_duplicates('identifiant_unique', keep='first', inplace=True)

    metadata = {
        'number_of_duplicates': number_of_duplicates,
        'duplicate_ids': list(duplicate_ids),
        'added_rows': len(df)
    }

    return {'df': df, 'metadata': metadata}


fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    op_kwargs={"dataset": "donnees-eo-refashion"},
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_from_postgresql',
    python_callable=load_data_from_postgresql,
    dag=dag,
)

create_actors_task = PythonOperator(
    task_id='create_actors',
    python_callable=create_actors,
    dag=dag,
)

create_proposition_services_task = PythonOperator(
    task_id='create_proposition_services',
    python_callable=create_proposition_services,
    dag=dag,
)

create_proposition_services_sous_categories_task = PythonOperator(
    task_id='create_proposition_services_sous_categories',
    python_callable=create_proposition_services_sous_categories,
    dag=dag,
)

write_data_task = PythonOperator(
    task_id='write_data_to_validate_into_dagruns',
    python_callable=write_to_dagruns,
    dag=dag,
)

serialize_to_json_task = PythonOperator(task_id='serialize_actors_to_records', python_callable=serialize_to_json,
                                        dag=dag)

([fetch_data_task,load_data_task] >>
 create_actors_task >>
 create_proposition_services_task >>
 create_proposition_services_sous_categories_task >> serialize_to_json_task >> write_data_task)
