from __future__ import annotations

from datetime import datetime

import pytz
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from google_play_review_lib.load import GooglePlayReviews

with DAG(
    "google_play_reviews",
    description="google_play_reviews",
    schedule_interval="0 5 * * *",
    start_date=datetime(2023, 1, 1, tzinfo=pytz.utc),
    catchup=False,
) as dag:

    def load_data(**kwargs):
        GooglePlayReviews(
            clickhouse_http=kwargs.get("clickhouse_http"),
            end_date_excl=datetime.now(pytz.utc).date(),
        )()

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={
            "clickhouse_http": Connection.get_connection_from_secrets(
                "clickhouse_http_debug"
            ),
        },
    )
