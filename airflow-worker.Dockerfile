FROM apache/airflow:2.8.2

CMD ["celery", "worker"]