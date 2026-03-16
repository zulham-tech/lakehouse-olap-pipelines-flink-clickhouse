## Project 6 — NASA Space Data Pipeline (Delta Lake)

> **Type:** Batch | **Difficulty:** Intermediate  
> **Stack:** NASA API → Kafka → PySpark → Delta Lake (S3) → BigQuery → Airflow

### Data Source
NASA Open API (`api.nasa.gov`) — free, use DEMO_KEY or register for 1,000 req/hr.  
3 streams: APOD (daily photo since 1995) + NeoWs (near-earth asteroids) + DONKI (space weather events).

### Architecture
```
NASA Open API (free · DEMO_KEY)
    ↓ (3 endpoints: APOD · NeoWs · DONKI)
Kafka [nasa-apod · nasa-neo-asteroids · nasa-donki-spaceweather]
    ↓ (PySpark batch)
PySpark → hazard classification + size category
    ↓ (MERGE upsert)
Delta Lake on S3 (ACID Lakehouse)
    → MERGE upsert · time-travel · ZORDER · VACUUM
    ↓ (daily Airflow sync)
BigQuery [apod_daily · neo_asteroids · donki_events]
```

### Delta Lake Features Used
| Feature | Purpose |
|---|---|
| MERGE upsert | No duplicates on daily reruns |
| Time-travel | `VERSION AS OF 0` for historical audits |
| ZORDER | Faster queries by date + event_id |
| VACUUM | Auto-cleanup old versions after 7 days |

---

## Project 7 — NYC Yellow Taxi Real-Time Analytics

> **Type:** Streaming | **Difficulty:** Intermediate  
> **Stack:** NYC TLC API → Kafka → Apache Flink → ClickHouse → Airflow

### Data Source
NYC TLC Open Data Socrata API (`data.cityofnewyork.us`) — free, no API key.  
1M+ Yellow Taxi trips/month. Fields: pickup/dropoff, location, fare, tip, distance.

### Architecture
```
NYC TLC Socrata API (free · no key)
    ↓ (hourly batch fetch, 2,000 trips/run)
Kafka [nyc-taxi-trips · GZIP]
    ↓ (Apache Flink SQL)
Flink: 5-min tumbling window per pickup location
    → trip_count · avg_fare · avg_tip_rate · total_revenue
    ↓
ClickHouse (MergeTree columnar OLAP)
    taxi_trips (raw)        → partition YYYYMM · ORDER BY pickup+location
    trip_5min_agg (Flink)   → ReplacingMergeTree
```

### Why ClickHouse?
Sub-100ms aggregation on millions of rows. Standard OLTP databases like PostgreSQL would take seconds for the same GROUP BY queries.

---

## Project 8 — USGS Global Earthquake Monitor

> **Type:** Streaming | **Difficulty:** Intermediate  
> **Stack:** USGS API → Kafka → PySpark → Elasticsearch + Kibana → Airflow

### Data Source
USGS Earthquake Hazards API (`earthquake.usgs.gov/fdsnws`) — free, no key, GeoJSON.  
~10,000–15,000 earthquakes/month. M≥1.0 globally. Updated every 5 minutes.

### Architecture
```
USGS Earthquake API (free · no key · GeoJSON)
    ↓ (every 30 min · M≥1.0 globally)
Kafka [usgs-earthquakes · GZIP]
    ↓ (PySpark Streaming · 30s trigger)
PySpark: depth_category · tsunami_risk · is_significant
    ↓ (Elasticsearch bulk index)
Elasticsearch: geo_point mapping
    → geo_distance radius queries
    → full-text search on place
    → magnitude aggregations
    ↓
Kibana: geospatial heatmaps · magnitude timelines
```

### Elasticsearch Geospatial Query Example
```json
{ "geo_distance": { "distance": "2000km",
    "location": { "lat": -6.2, "lon": 106.8 } } }
```

---

## Project 9 — IMF World Economic Outlook ELT Pipeline

> **Type:** Batch ELT | **Difficulty:** Intermediate  
> **Stack:** IMF API → S3 (raw) → DuckDB → BigQuery → Airflow

### Data Source
IMF DataMapper API (`imf.org/external/datamapper/api/v1`) — free, no API key.  
8 WEO indicators × 190+ countries × 25 years = ~50,000 records/run. Includes forecasts to 2027.

### Architecture
```
IMF DataMapper API (free · no key)
    ↓ (E: Extract raw)
Amazon S3 [imf/raw/indicators/dt=DATE/*.parquet]  ← immutable raw zone
    ↓ (T: Transform)
DuckDB (in-memory, no cluster)
    → INSTALL httpfs → read S3 Parquet directly
    → PIVOT long→wide · health score · income group
    ↓ (L: Load)
BigQuery [imf_economic_analytics.weo_enriched]
    DELETE + INSERT idempotent · partitioned by batch_date
```

### ELT vs ETL
| | ETL | ELT (this project) |
|---|---|---|
| Transform where | Before loading (Spark cluster) | After loading (DuckDB/SQL) |
| Infrastructure | Spark cluster needed | DuckDB — any laptop |
| Raw data retained | ❌ | ✅ (S3 Parquet) |

---

## Project 10 — Wikimedia Trending Topics (Lambda Architecture)

> **Type:** Lambda (Streaming + Batch) | **Difficulty:** Advanced  
> **Stack:** Wikimedia API + SSE → Kafka → Flink → Cassandra + Snowflake → Airflow

### Data Sources
1. Wikimedia Pageviews API — top 50 articles × 10 languages × daily
2. Wikimedia SSE Stream (`stream.wikimedia.org`) — live Wikipedia edit events

### Lambda Architecture
```
Wikimedia API       Wikimedia SSE Stream
(top articles)      (live edits, true push)
     ↓                      ↓
Kafka [pageviews]   Kafka [recentchanges]
     ↓                      ↓
PySpark Batch       Apache Flink
YoY change %        10-min window
                    trending_score =
                    human_edits×3 + bot×1
     ↓                      ↓
Snowflake           Cassandra
ARTICLE_PAGEVIEWS   trending_articles TTL 24h
(analytics)         (speed, ms reads)
```

### Trending Score
Articles with high edit velocity (especially non-bot) = breaking news or trending topics.  
`trending_score = (human_edits × 3) + (bot_edits × 1)` per 10-minute window.

### Airflow: 2 Separate DAGs
| DAG | Schedule | Function |
|---|---|---|
| `wikimedia_speed_layer` | Hourly | SSE + pageviews → Kafka → Flink → Cassandra |
| `wikimedia_batch_layer` | Daily 22:00 WIB | Pageviews → Kafka → PySpark → Snowflake |

---

## Full Tech Stack Summary (Projects 6–10)

| Technology | Project(s) | New vs Previous |
|---|---|---|
| **Delta Lake** | P6 | 🆕 ACID lakehouse on S3 |
| **Apache Flink** | P7, P10 | 🆕 Stream processing |
| **ClickHouse** | P7 | 🆕 Columnar OLAP |
| **Elasticsearch** | P8 | 🆕 Full-text + geospatial |
| **Kibana** | P8 | 🆕 Geospatial dashboards |
| **DuckDB** | P9 | 🆕 In-memory ELT engine |
| **Wikimedia SSE** | P10 | 🆕 True event-driven stream |
| **Apache Kafka** | All | ✅ Same as before |
| **PySpark** | P6, P8, P9, P10 | ✅ Same as before |
| **Apache Cassandra** | P10 | ✅ Same as P5 |
| **Snowflake** | P10 | ✅ Same as P4, P5 |
| **BigQuery** | P6, P9 | ✅ Same as P1, P3 |

---

*Ahmad Zulham Hamdan | Data Engineer | Kafka · PySpark · Airflow · Delta Lake · Flink · Elasticsearch · DuckDB*  
*📧 zulham.va@gmail.com | 🔗 linkedin.com/in/ahmad-zulham-hamdan-665170279*
