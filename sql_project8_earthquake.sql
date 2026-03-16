-- Project 8: USGS Earthquake — Elasticsearch Mappings + Kibana Queries

-- Elasticsearch Index Mapping (PUT /earthquakes-enriched)
{
  "mappings": {
    "properties": {
      "event_id":        { "type": "keyword" },
      "event_time":      { "type": "date" },
      "magnitude":       { "type": "float" },
      "magnitude_class": { "type": "keyword" },
      "depth_km":        { "type": "float" },
      "location":        { "type": "geo_point" },
      "place":           { "type": "text", "analyzer": "standard",
                          "fields": {"keyword": {"type": "keyword"}} },
      "tsunami_flag":    { "type": "integer" },
      "tsunami_risk":    { "type": "keyword" },
      "alert_level":     { "type": "keyword" },
      "depth_category":  { "type": "keyword" },
      "sig":             { "type": "integer" },
      "felt_reports":    { "type": "integer" },
      "is_significant":  { "type": "boolean" },
      "ingested_at":     { "type": "date" }
    }
  }
}

-- Elasticsearch Query 1: M5+ earthquakes near Indonesia (geo_distance)
GET /earthquakes-enriched/_search
{
  "query": {
    "bool": {
      "must": [{ "range": { "magnitude": { "gte": 5.0 } } }],
      "filter": [{
        "geo_distance": {
          "distance": "2000km",
          "location": { "lat": -2.548926, "lon": 118.014863 }
        }
      }]
    }
  },
  "sort": [{ "magnitude": { "order": "desc" } }],
  "size": 20
}

-- Elasticsearch Query 2: Magnitude class aggregation
GET /earthquakes-enriched/_search
{
  "size": 0,
  "aggs": {
    "by_magnitude_class": {
      "terms": { "field": "magnitude_class", "size": 10 }
    },
    "avg_daily_count": {
      "date_histogram": { "field": "event_time", "calendar_interval": "day" }
    }
  }
}

-- Elasticsearch Query 3: Tsunami risk events
GET /earthquakes-enriched/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "tsunami_risk": "high" } },
        { "range": { "event_time": { "gte": "now-30d" } } }
      ]
    }
  },
  "sort": [{ "magnitude": "desc" }],
  "size": 10
}
