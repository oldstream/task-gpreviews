import logging
import sqlite3
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime
from sqlite3 import Connection as SqliteConnection
from typing import Any, Dict, Iterable

import clickhouse_connect
import pytz
from clickhouse_connect.driver.client import Client as ClickhouseClient
from google_play_scraper import Sort, reviews

APP_ID = "org.telegram.messenger"
LANGS = ("ru", "en")
MINDATE_EXCL = date(2023, 12, 31)  # Минимальная дата для загрузки

logger = logging.getLogger(__name__)


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

    def __setattr__(self, __name: str, __value: Any) -> None:
        """Sqlite конвертирует date в str, обратная конверсия"""

        if __name == "event_date" and isinstance(__value, str):
            __value = datetime.strptime(__value, "%Y-%m-%d")
        object.__setattr__(self, __name, __value)


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
) -> Iterable[RawRecord]:
    """Чтение записей из API Google Play"""

    api_result, continuation_token = reviews(
        app_id, lang=language, sort=Sort.NEWEST, count=100
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
        self.clickhouse_http = clickhouse_http
        self.end_date_excl = end_date_excl
        self.app_id = app_id
        self.langs = langs

    def _sqlite_client(self) -> SqliteConnection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        return conn

    def _clickhouse_client(self) -> ClickhouseClient:
        return clickhouse_connect.get_client(
            host=self.clickhouse_http.host,
            username=self.clickhouse_http.login,
            password=self.clickhouse_http.password or "",
            port=self.clickhouse_http.port,
            database=self.clickhouse_http.schema,
        )

    def __call__(self):
        with self._sqlite_client() as self.conn, self._clickhouse_client() as self.clickhouse:
            self.insert_datetime = datetime.now(pytz.utc)
            self.insert_date = self.insert_datetime.date()
            for language in self.langs:
                max_event_date = self._get_max_loaded_date(language)
                logger.info(f"language={language}, max_event_date={max_event_date}")
                self._make_raw_table()
                self._extract_raw_records(max_event_date, language)
                cdm_records = self._transform_to_cdm_records()
                self._load_cdm_records(cdm_records)

    def _get_max_loaded_date(self, language: str) -> date:
        """Возвращает максимально загруженную дату из витрины для указанного языка"""

        result = self.clickhouse.query(
            """
            SELECT max(event_date) AS  max_event_date 
            FROM public_reviews
            WHERE language = {lang:String}
            """,
            parameters=dict(lang=language),
        )
        return result.result_rows[0][0]

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

    def _extract_raw_records(self, max_event_date: date, language: str):
        """Загрузка RAW записей"""

        cursor = self.conn.cursor()
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

    def _transform_to_cdm_records(self) -> Iterable[CdmRecord]:
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
            yield CdmRecord(**dict(record))

    def _load_cdm_records(self, cdm_records: Iterable[CdmRecord]):
        """Вставка записей в витрину"""

        cdm_fields = [f.name for f in fields(CdmRecord)]

        def prep_row(rows):
            """Трансформация потока Cdm записей в список, пригодный для загрузки"""
            nonlocal cdm_fields
            for row in rows:
                yield [getattr(row, c) for c in cdm_fields] + [
                    self.insert_datetime,
                    self.insert_date,
                ]

        data = list(prep_row(cdm_records))
        self.clickhouse.insert(
            "public_reviews",
            data,
            column_names=cdm_fields + ["insert_datetime", "insert_date"],
        )
