FROM apache/airflow:2.8.2

COPY ./airflow-init.sh .

CMD ["./airflow-init.sh"]
