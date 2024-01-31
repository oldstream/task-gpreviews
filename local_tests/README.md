# Локальное тестирование

Установить зависимости
```shell
pip install -r requirements.txt --constraint constraints.txt
```

Инициализировать базу данных Airflow
```shell
airflow db migrate
```

Отредактировать ~/airflow/airflow.cfg, указать абсолютный путь до каталога .../dags/ 
```
dags_folder = /.../task-gpreviews/dags
```

Добавить connection для локального Clickhouse:
```shell
airflow connections add clickhouse_http_local --conn-type Generic --conn-schema default --conn-host 192.168.5.138 --conn-port 8123 --conn-login default
```

Запуск локального Clickhouse
```shell
cd ./local_tests/clickhouse
docker-compose up -d
```

## Запуск загрузки данных для дебаггера с возможностью breakpoints

Запустить `run_etl.py`. Корень проекта должен быть в PYTHONPATH.

## Запуск DAG
```shell
airflow dags test google_play_reviews
```
