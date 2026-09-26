FROM apache/airflow:2.9.1-python3.11

USER airflow
WORKDIR /opt/airflow

COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root database/ /opt/airflow/database/
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root airflow/plugins/ /opt/airflow/plugins/


