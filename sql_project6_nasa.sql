-- Project 6: NASA Space Data Pipeline — Delta Lake + BigQuery
-- DDL + Analytics Queries

-- BigQuery DDL: APOD table
CREATE TABLE IF NOT EXISTS `your_project.nasa_space_analytics.apod_daily` (
  apod_date   DATE,
  title       STRING,
  explanation STRING,
  media_type  STRING,
  url         STRING,
  copyright   STRING,
  batch_date  DATE,
  ingested_at TIMESTAMP
) PARTITION BY apod_date;

-- BigQuery DDL: NEO asteroids
CREATE TABLE IF NOT EXISTS `your_project.nasa_space_analytics.neo_asteroids` (
  neo_id                     STRING,
  neo_name                   STRING,
  close_approach_date        DATE,
  is_potentially_hazardous   BOOL,
  estimated_diameter_max_km  FLOAT64,
  relative_velocity_kmh      FLOAT64,
  miss_distance_km           FLOAT64,
  hazard_level               STRING,
  size_category              STRING,
  batch_date                 DATE,
  ingested_at                TIMESTAMP
) PARTITION BY close_approach_date;

-- BigQuery DDL: DONKI space weather events
CREATE TABLE IF NOT EXISTS `your_project.nasa_space_analytics.donki_events` (
  event_id        STRING,
  event_type      STRING,
  begin_time      TIMESTAMP,
  peak_time       TIMESTAMP,
  class_type      STRING,
  source_location STRING,
  kp_index        FLOAT64,
  batch_date      DATE,
  ingested_at     TIMESTAMP
);

-- QUERY 1: Hazardous asteroids this month
SELECT neo_name, close_approach_date,
       ROUND(miss_distance_km/1000000, 2)   AS miss_dist_million_km,
       ROUND(estimated_diameter_max_km, 3)  AS diameter_km,
       ROUND(relative_velocity_kmh/1000, 1) AS velocity_k_kmh,
       hazard_level
FROM `your_project.nasa_space_analytics.neo_asteroids`
WHERE is_potentially_hazardous = TRUE
  AND close_approach_date >= DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY miss_distance_km ASC LIMIT 20;

-- QUERY 2: Monthly hazardous asteroid count
SELECT DATE_TRUNC(close_approach_date, MONTH)  AS month,
       COUNT(*)                                AS total_neo,
       COUNTIF(is_potentially_hazardous)       AS hazardous_count,
       ROUND(AVG(miss_distance_km)/1000000, 2) AS avg_miss_dist_m_km
FROM `your_project.nasa_space_analytics.neo_asteroids`
GROUP BY 1 ORDER BY 1 DESC LIMIT 24;

-- QUERY 3: Solar flare class distribution this year
SELECT class_type,
       COUNT(*)                  AS event_count,
       MIN(begin_time)           AS first_event,
       MAX(peak_time)            AS latest_peak
FROM `your_project.nasa_space_analytics.donki_events`
WHERE event_type = 'solar_flare'
  AND EXTRACT(YEAR FROM begin_time) = EXTRACT(YEAR FROM CURRENT_DATE())
GROUP BY class_type ORDER BY event_count DESC;

-- QUERY 4: Delta Lake time-travel check (run in Spark)
-- DESCRIBE HISTORY delta.`s3a://your-bucket/delta/neo_asteroids`;
-- SELECT * FROM delta.`s3a://your-bucket/delta/neo_asteroids` VERSION AS OF 0;
