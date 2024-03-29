# Que faire de mes objets - Data Platform

Ce projet contient l'environnement d'execution d'Airflow et les DAGs qui sont exécutés sur Airflow

## Lancement de Airflow en local

Executer docker compose:

```sh
docker compose up
```

docker compose lancera :

- une base de données postgres nécessaire à Airflow
- un webserver airflow
- un scheduler airflow en mode LocalExecutor

accéder à l'interface d'Airflow en local [http://localhost:8080](http://localhost:8080) ; identifiant/mot de passe : airflow / airflow

## Mise à jour du scheduler et du webserver sur CleverCloud

Airflow tourne sur CleverCloud et utilise les services suivant:

- qfdmo-airflow-webserver (instance docker): interface d'airflow
- qfdmo-airflow-scheduler (instance docker): scheduler d'airflow, fait tourner les dags car on est configuré en LocalExecutor
- qfdmo-storage : S3 pour stocker les logs des dags
- qfdmo-airflow-postgres : base de données nécessaire au fonctionnelment d'airflow

## Pour déployer airflow

- configururer sa clé ssh dans l'interface de clevercloud (cf.doc clevercloud)
- configurer un "remote repository" pour `qfdmo-airflow-webserver` (qu'on nommera clevercloud-webserver) pour ce repository
- configurer un "remote repository" pour `qfdmo-airflow-scheduler` (qu'on nommera clevercloud-scheduler) pour ce repository
- pousser le code souhaité sur la branch master des 2 repository ci-dessus

```sh
git remote add clevercloud-webserver git+ssh://git@push-n3-par-clevercloud-customers.services.clever-cloud.com/app_efd2802a-1773-48e0-987e-7a6dffb929d1.git
git remote add clevercloud-scheduler git+ssh://git@push-n3-par-clevercloud-customers.services.clever-cloud.com/app_fda5d606-44d9-485f-a1b4-1f7007bc3bec.git
git push clevercloud-webserver mabranch:master
git push clevercloud-scheduler mabranch:master
```

Pour que les logs du scheduler soit stocké sur S3, les instances CleverCloud sont lancés avec les variables d'environnement:

```txt
AIRFLOW__LOGGING__REMOTE_LOGGING=true
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://qfdmo-airflow-logs
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=s3logs
AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=false
```

`s3logs` est une connection configuré dans l'interface d'Airflow

![Configuration d'une connexion à Cellar (stockage s3 de clevercloud) dans Airflow](./img/airflow-s3-connection-configuration.png)

Attention à ajouter le paramètre enfpoint_url pour le stockage Cellar de CleverCloud

## Reste à faire

- [ ] Aujourd'hui, on a 1 seule buket de log pour tout les environnement
- [ ] Strategie pour publier des dag de preprod et de prod en les identifiant et en permettant des config différentes
