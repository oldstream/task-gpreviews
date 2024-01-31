FROM apache/airflow:2.8.1
COPY requirements.txt /
COPY constraints.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt --constraint /constraints.txt
