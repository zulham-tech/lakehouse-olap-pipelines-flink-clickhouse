-- Project 7: NYC Taxi — ClickHouse DDL + Analytics

-- ClickHouse: Raw trips table
CREATE TABLE IF NOT EXISTS nyc_taxi_analytics.taxi_trips (
    vendor_id            String,
    pickup_datetime      DateTime,
    dropoff_datetime     DateTime,
    passenger_count      UInt8,
    trip_distance_miles  Float32,
    pu_location_id       UInt16,
    do_location_id       UInt16,
    payment_type_name    LowCardinality(String),
    fare_amount          Float32,
    tip_amount           Float32,
    total_amount         Float32,
    tip_rate_pct         Float32,
    duration_minutes     UInt16,
    speed_mph            Float32,
    congestion_surcharge Float32,
    ingested_at          DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_datetime, pu_location_id)
SETTINGS index_granularity = 8192;

-- ClickHouse: 5-min aggregation table
CREATE TABLE IF NOT EXISTS nyc_taxi_analytics.trip_5min_agg (
    window_start     DateTime,
    window_end       DateTime,
    pu_location_id   UInt16,
    trip_count       UInt32,
    avg_fare         Float32,
    avg_tip_rate     Float32,
    avg_duration_min Float32,
    total_revenue    Float32,
    avg_speed_mph    Float32,
    credit_card_pct  Float32
) ENGINE = ReplacingMergeTree(window_start)
ORDER BY (window_start, pu_location_id);

-- QUERY 1: Top 10 busiest pickup zones last hour
SELECT pu_location_id,
       count()          AS trip_count,
       avg(fare_amount) AS avg_fare,
       avg(tip_rate_pct) AS avg_tip_pct,
       sum(total_amount) AS total_revenue
FROM taxi_trips
WHERE pickup_datetime >= now() - INTERVAL 1 HOUR
GROUP BY pu_location_id ORDER BY trip_count DESC LIMIT 10;

-- QUERY 2: Revenue by payment type today
SELECT payment_type_name,
       count()           AS trips,
       sum(total_amount) AS revenue,
       avg(tip_rate_pct) AS avg_tip_pct,
       avg(trip_distance_miles) AS avg_distance
FROM taxi_trips
WHERE toDate(pickup_datetime) = today()
GROUP BY payment_type_name ORDER BY revenue DESC;

-- QUERY 3: Hourly trip pattern
SELECT toHour(pickup_datetime)       AS hour_of_day,
       count()                       AS trip_count,
       avg(fare_amount)              AS avg_fare,
       avg(duration_minutes)         AS avg_duration
FROM taxi_trips
WHERE pickup_datetime >= today() - 7
GROUP BY hour_of_day ORDER BY hour_of_day;
