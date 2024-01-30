CREATE TABLE IF NOT EXISTS public_reviews
(
    event_date      Date,
    language        String,
    reviews_count   Int32,
    min_score       Float32,
    avg_score       Float32,
    max_score       Float32,
    insert_date     Date,
    insert_datetime DateTime
)
ENGINE = MergeTree()
ORDER BY (event_date, language)
;
