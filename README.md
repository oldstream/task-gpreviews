# task-gpreviews

## Зависимости
Python 3.9
Aitflow 

## Тестирование

### Clickhouse

```shell
cd ./clickhouse
docker-compose up -d
```

### Установка Airflow
```shell
pip install "apache-airflow==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.9.txt"
```

```shell
export AIRFLOW_HOME=~/airflow
airflow standalone
```

UI: http://localhost:8080
user: admin
password: смотри в AIRFLOW_HOME

```shell
cd ~/airflow
ln -s /Users/vik-a-makarov/mine/task-gpreviews/dags dags
```


Запуск DAG
```shell
# run your first task instance
airflow tasks test example_bash_operator runme_0 2015-01-01
# run a backfill over 2 days
airflow dags backfill example_bash_operator \
    --start-date 2015-01-01 \
    --end-date 2015-01-02
```
