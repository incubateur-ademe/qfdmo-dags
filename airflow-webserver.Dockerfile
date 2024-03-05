# Use the Apache Airflow (version:2.8.2) image from the DockerHub
FROM apache/airflow:2.8.2

# # Set the AIRFLOW_HOME variable
# ENV AIRFLOW_HOME=/usr/local/airflow

# # Copy the dags, logs, config, and plugins directories to the appropriate locations
# COPY ./dags /opt/airflow/dags
# COPY ./logs /opt/airflow/logs
# COPY ./config /opt/airflow/config
# COPY ./plugins /opt/airflow/plugins
# COPY ./airflow-init.sh .

# RUN ./airflow-init.sh

# Use user airflow
RUN chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow
USER ${AIRFLOW_UID:-50000}:0
EXPOSE 8080

# Start the web server on port 8080
CMD ["webserver", "--port", "8080"]
