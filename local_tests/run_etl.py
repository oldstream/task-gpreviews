from datetime import datetime

import pytz
from airflow.models import Connection

from dags.google_play_review_lib.load import GooglePlayReviews

if __name__ == "__main__":
    """Для теста"""

    clickhouse_http = Connection.get_connection_from_secrets("clickhouse_http_local")
    GooglePlayReviews(
        clickhouse_http=clickhouse_http,
        end_date_excl=datetime.now(pytz.utc).date(),
    )()
