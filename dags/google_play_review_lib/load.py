import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable

import pytz
from google_play_scraper import Sort, reviews
from infi.clickhouse_orm import database, engines, fields, models

APP_ID = "org.telegram.messenger"
LANGS = ("ru", "en")
MINDATE_EXCL = date(2023, 12, 31)  # Минимальная дата для загрузки, исключительно


@dataclass
class RawRecord:
    """Сырая запись из API"""

    event_date: date
    language: str
    score: float


@dataclass
class CdmRecord:
    """Запись со статистикой для витрины в Clickhouse"""

    event_date: date
    language: str
    reviews_count: int
    min_score: float
    avg_score: float
    max_score: float
    insert_date: date
    insert_datetime: datetime


class PublicReview(models.Model):
    """Класс, описывающий витрину для infi.clickhouse_orm"""

    event_date = fields.DateField()
    language = fields.StringField()
    reviews_count = fields.Int64Field()
    min_score = fields.Float32Field()
    avg_score = fields.Float32Field()
    max_score = fields.Float32Field()
    insert_date = fields.DateField()
    insert_datetime = fields.DateTimeField()
    engine = engines.MergeTree("event_date", order_by=("event_date", "language"))

    @classmethod
    def table_name(cls):
        return "public_reviews"


def make_raw_record(api_record: Dict, language: str) -> RawRecord:
    """Создание сырой записи из API записи"""

    return RawRecord(
        event_date=api_record["at"].date(),
        language=language,
        score=api_record["score"],
    )


def browse_api(
    app_id: str,
    language: str,
    start_date_excl: date,
    end_date_excl: date,
) -> Iterable:
    """Чтение записей из API"""

    api_result, continuation_token = reviews(
        app_id, lang=language, sort=Sort.NEWEST, count=3
    )  # type: ignore
    while api_result:
        for api_record in api_result:
            raw_record = make_raw_record(api_record, language)
            if raw_record.event_date <= start_date_excl:
                return
            if raw_record.event_date < end_date_excl:
                yield raw_record
        api_result, continuation_token = reviews(
            app_id, continuation_token=continuation_token
        )


class GooglePlayReviews:
    """Класс, реализующий ETL для Google Play Reviews"""

    def __init__(
        self,
        clickhouse_http,
        end_date_excl: date,
        app_id=APP_ID,
        langs=LANGS,
    ) -> None:
        self.end_date_excl = end_date_excl
        self.app_id = app_id
        self.langs = langs
        self.insert_datetime = datetime.now(pytz.utc)
        self.insert_date = self.insert_datetime.date()
        self.clickhouse = database.Database(
            db_name=clickhouse_http.schema,
            db_url=f"{clickhouse_http.host}:{clickhouse_http.port}",
            username=clickhouse_http.login,
            password=clickhouse_http.password,
            autocreate=False,
            timeout=600,
        )

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        max_event_date = self._get_max_loaded_date()
        with sqlite3.connect(":memory:") as self.conn:
            self.conn.row_factory = sqlite3.Row
            self._make_raw_table()
            self._load_raw_records(max_event_date)
            self._insert_cdm_records(self._read_cdm_records())

    def _get_max_loaded_date(self) -> date:
        """Возвращает максимально загруженную дату из витрины"""

        rows = self.clickhouse.select(
            """
            SELECT max(event_date) AS  max_event_date FROM public_reviews
            """
        )
        row = next(rows)
        return row.max_event_date

    def _make_raw_table(self):
        """Создание пустой RAW таблицы"""

        cursor = self.conn.cursor()
        cursor.executescript(
            """
            DROP TABLE IF EXISTS raw_records;
            CREATE TABLE raw_records (                          
                event_date date,
                language text,
                score float
            )
            """
        )
        self.conn.commit()

    def _load_raw_records(self, max_event_date):
        """Загрузка RAW записей"""

        cursor = self.conn.cursor()
        for language in self.langs:
            for raw_record in browse_api(
                app_id=self.app_id,
                language=language,
                start_date_excl=max(MINDATE_EXCL, max_event_date),
                end_date_excl=self.end_date_excl,
            ):
                cursor.execute(
                    """
                    INSERT INTO raw_records (event_date, language, score) 
                    VALUES (:event_date, :language, :score)
                    """,
                    asdict(raw_record),
                )
        self.conn.commit()

    def _read_cdm_records(self) -> Iterable:
        """Расчет и поставка записей для витрины"""

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                event_date,
                language,
                count(*) 		AS reviews_count,
                min(score) 		AS min_score,
                avg(score)		AS avg_score,
                max(score)		AS max_score
            FROM raw_records
            GROUP BY event_date, language
            ORDER BY event_date, language
            """
        )
        for record in cursor.fetchall():
            yield PublicReview(
                **dict(
                    **dict(record),
                    insert_date=self.insert_date,
                    insert_datetime=self.insert_datetime,
                )
            )

    def _insert_cdm_records(self, cdm_records: Iterable):
        """Вставка записей в витрину"""

        self.clickhouse.insert(cdm_records)


if __name__ == "__main__":
    """Для теста"""

    @dataclass
    class Clickhouse_http:
        schema = "default"
        host = "http://localhost"
        port = "8123"
        login = "default"
        password = None

    GooglePlayReviews(
        clickhouse_http=Clickhouse_http(),
        end_date_excl=datetime.now(pytz.utc).date(),
    )()
