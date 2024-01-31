# task-gpreviews

Загрузка данных по ревью приложения в Google Play, расчет витрины и загрузка ее в Clickhouse.

В dags/google_play_review_lib/etl.py указаны неизменяемые параметры загрузки:

```python
APP_ID = "org.telegram.messenger"
LANGS = ("ru", "en")
MINDATE_EXCL = date(2023, 12, 31)  # Минимальная дата для загрузки
```

Загрузка осуществляется начиная с даты последней записи в витрине плюс один до сегодняшнего дня исключительно. Первая
загрузка начнется с даты MINDATE_EXCL.

## Запуск DAG

Запуск Airflow
```shell
docker-compose build
docker-compose up -d
```

Web-интерфей Airflow http://localhost:8080

    user: airflow
    password: airflow

Создать Connection:

    Connection Id: clickhouse_http
    Connection Type: Generic
    Host:
    Schema:
    Login:
    Password:
    Port:

Включить DAG google_play_reviews. Первый раз стартует сразу автоматически, затем по расписанию каждый день в 5 утро UTC.
