FROM apache/airflow:2.8.2

# Use user airflow
RUN chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow
USER ${AIRFLOW_UID:-50000}:0

COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt

# Copy the dags, logs, config, and plugins directories to the appropriate locations
COPY ./dags/ /opt/airflow/dags/
COPY ./config/ /opt/airflow/config/
COPY ./plugins/ /opt/airflow/plugins/
RUN mkdir -p /opt/airflow/logs/

CMD ["scheduler"]
