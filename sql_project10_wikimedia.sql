-- Project 10: Wikimedia Trending Pipeline — Cassandra CQL + Snowflake SQL

-- Cassandra Keyspace + Tables
CREATE KEYSPACE IF NOT EXISTS wikimedia_trending
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wikimedia_trending.trending_articles (
    language          TEXT,
    window_start      TIMESTAMP,
    article_title     TEXT,
    edit_count        INT,
    human_edit_count  INT,
    total_byte_change INT,
    trending_score    INT,
    ingested_at       TEXT,
    PRIMARY KEY (language, window_start, trending_score)
) WITH CLUSTERING ORDER BY (window_start DESC, trending_score DESC)
  AND default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS wikimedia_trending.article_pageviews (
    language      TEXT,
    view_date     TEXT,
    article_title TEXT,
    page_views    INT,
    rank          INT,
    PRIMARY KEY (language, view_date, article_title)
) WITH CLUSTERING ORDER BY (view_date DESC);

-- Cassandra: Top trending articles in English right now
SELECT language, article_title, edit_count, human_edit_count, trending_score
FROM wikimedia_trending.trending_articles
WHERE language = 'en'
  AND window_start >= toTimestamp(now()) - 3600s
LIMIT 20;

-- Snowflake DDL
CREATE DATABASE IF NOT EXISTS WIKIMEDIA_DW;
CREATE SCHEMA  IF NOT EXISTS WIKIMEDIA_DW.ANALYTICS;

CREATE TABLE IF NOT EXISTS WIKIMEDIA_DW.ANALYTICS.ARTICLE_PAGEVIEWS (
    ARTICLE_ID    VARCHAR(500),
    LANGUAGE      VARCHAR(5),
    ARTICLE_TITLE VARCHAR(500),
    VIEW_DATE     DATE,
    RANK          INT,
    PAGE_VIEWS    INT,
    INGESTED_AT   TIMESTAMP_NTZ
) CLUSTER BY (LANGUAGE, VIEW_DATE);

-- Snowflake Query 1: Top articles by language last 7 days
SELECT LANGUAGE, ARTICLE_TITLE,
       SUM(PAGE_VIEWS)   AS total_views,
       AVG(RANK)         AS avg_rank,
       COUNT(DISTINCT VIEW_DATE) AS days_in_top
FROM WIKIMEDIA_DW.ANALYTICS.ARTICLE_PAGEVIEWS
WHERE VIEW_DATE >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY LANGUAGE, ARTICLE_TITLE
ORDER BY LANGUAGE, total_views DESC;

-- Snowflake Query 2: Cross-language trending (same article in multiple languages)
SELECT ARTICLE_TITLE,
       COUNT(DISTINCT LANGUAGE)   AS language_count,
       SUM(PAGE_VIEWS)            AS total_views,
       LISTAGG(LANGUAGE, ', ')    AS languages
FROM WIKIMEDIA_DW.ANALYTICS.ARTICLE_PAGEVIEWS
WHERE VIEW_DATE = CURRENT_DATE() - 1
GROUP BY ARTICLE_TITLE
HAVING COUNT(DISTINCT LANGUAGE) >= 3
ORDER BY language_count DESC, total_views DESC
LIMIT 20;
