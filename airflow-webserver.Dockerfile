# Use the Apache Airflow (version:2.8.2) image from the DockerHub
FROM apache/airflow:2.8.2

# Use user airflow
RUN chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow
USER ${AIRFLOW_UID:-50000}:0

# Copy the dags, logs, config, and plugins directories to the appropriate locations
COPY ./dags/ /opt/airflow/dags/
COPY ./config/ /opt/airflow/config/
COPY ./plugins/ /opt/airflow/plugins/
RUN mkdir -p /opt/airflow/logs/

EXPOSE 8080

RUN echo "STARTING WEBSERVER --port 8080"

# Start the web server on port 8080
CMD ["webserver", "--port", "8080"]
