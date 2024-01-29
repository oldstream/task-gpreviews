import sqlite3
from dataclasses import asdict, dataclass
from datetime import MINYEAR, date, datetime
from typing import Dict, Iterable, List

import pytz
from google_play_scraper import Sort, reviews
from infi.clickhouse_orm import database, engines, fields, models

APP_ID = "org.telegram.messenger"
LANGS = ("ru", "en")
MINDATE = date(MINYEAR, 1, 1)
UTC = pytz.utc
CLICKHOUSE = database.Database(
    db_name="default",
    db_url="http://localhost:8123",
    username="default",
    password=None,
    autocreate=False,
    timeout=600,
)


@dataclass
class RawRecord:
    event_date: date
    language: str
    score: float
    content: str


@dataclass
class CdmRecord:
    event_date: date
    language: str
    reviews_count: int
    min_score: float
    avg_score: float
    max_score: float
    insert_date: date
    insert_datetime: datetime


class PublicReview(models.Model):
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
    return RawRecord(
        event_date=api_record["at"].date(),
        language=language,
        score=api_record["score"],
        content=api_record["content"],
    )


def browse_api(app_id: str, language: str, till_date: date = MINDATE) -> Iterable:
    api_result, continuation_token = reviews(
        APP_ID, lang=language, sort=Sort.NEWEST, count=3
    )  # type: ignore
    while api_result:
        for api_record in api_result:
            raw_record = make_raw_record(api_record, language)
            if raw_record.event_date <= till_date:
                return
            yield raw_record
        api_result, continuation_token = reviews(
            APP_ID, continuation_token=continuation_token
        )


class GooglePlayReviews:
    def __init__(self, app_id, langs, till_date) -> None:
        self.app_id = app_id
        self.langs = langs
        self.till_date = till_date
        self.insert_datetime = datetime.now(UTC)
        self.insert_date = self.insert_datetime.date()
        with sqlite3.connect("sqlite.db") as self.conn:  # sqlite3.connect(':memory:')
            self.conn.row_factory = sqlite3.Row
            self._make_raw_table()
            self._load_raw_records()
            self._insert_cdm_records(self._read_cdm_records())

    def _make_raw_table(self):
        cursor = self.conn.cursor()
        cursor.executescript(
            """
            DROP TABLE IF EXISTS raw_records;
            CREATE TABLE raw_records (                          
                event_date date,
                language text,
                score float,
                content text
            )
            """
        )
        self.conn.commit()

    def _load_raw_records(self):
        cursor = self.conn.cursor()
        for language in self.langs:
            for raw_record in browse_api(
                app_id=self.app_id, language=language, till_date=self.till_date
            ):
                cursor.execute(
                    """
                    INSERT INTO raw_records (event_date, language, score, content) 
                    VALUES (:event_date, :language, :score, :content)
                    """,
                    asdict(raw_record),
                )
        self.conn.commit()

    # @staticmethod
    # def dict_factory(cursor, row):
    #     d = {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    #     return d

    def _read_cdm_records(self) -> Iterable:
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
        CLICKHOUSE.insert(cdm_records)


GooglePlayReviews(app_id=APP_ID, langs=LANGS, till_date=date(2024, 1, 20))
