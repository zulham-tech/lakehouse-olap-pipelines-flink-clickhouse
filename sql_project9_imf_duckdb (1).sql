-- Project 9: IMF WEO ELT Pipeline — DuckDB + BigQuery Queries

-- BigQuery DDL: WEO enriched table
CREATE TABLE IF NOT EXISTS `your_project.imf_economic_analytics.weo_enriched` (
  country_code          STRING,
  country_name          STRING,
  year                  INT64,
  is_forecast           BOOL,
  gdp_growth_pct        FLOAT64,
  inflation_pct         FLOAT64,
  unemployment_pct      FLOAT64,
  current_account_pct   FLOAT64,
  gross_debt_pct        FLOAT64,
  gdp_per_capita_usd    FLOAT64,
  export_growth_pct     FLOAT64,
  health_score          FLOAT64,
  income_group          STRING,
  batch_date            DATE,
  loaded_at             TIMESTAMP
) PARTITION BY batch_date;

-- DuckDB Query: Pivot check
SELECT country_code, year, gdp_real_growth_pct, inflation_avg_pct
FROM mart_weo_enriched
WHERE country_code IN ('IDN','USA','CHN','DEU','JPN')
  AND year BETWEEN 2020 AND 2026
ORDER BY country_code, year;

-- BigQuery Query 1: Economic health leaderboard (latest year)
SELECT country_name, income_group,
       ROUND(gdp_growth_pct, 2)    AS gdp_growth,
       ROUND(inflation_pct, 2)     AS inflation,
       ROUND(unemployment_pct, 2)  AS unemployment,
       ROUND(health_score, 2)      AS health_score,
       DENSE_RANK() OVER (ORDER BY health_score DESC) AS rank
FROM `your_project.imf_economic_analytics.weo_enriched`
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
  AND is_forecast = FALSE
ORDER BY health_score DESC LIMIT 20;

-- BigQuery Query 2: ASEAN economic comparison
SELECT country_code, country_name, year,
       ROUND(gdp_growth_pct, 2)    AS gdp_growth,
       ROUND(gdp_per_capita_usd,0) AS gdp_pc,
       ROUND(inflation_pct, 2)     AS inflation,
       income_group
FROM `your_project.imf_economic_analytics.weo_enriched`
WHERE country_code IN ('IDN','MYS','THA','PHL','VNM','SGP','MMR','KHM','LAO','BRN')
  AND year >= 2015
ORDER BY country_code, year;

-- BigQuery Query 3: GDP forecast vs actual
SELECT country_name, year,
       ROUND(gdp_growth_pct, 2) AS gdp_growth,
       is_forecast,
       ROUND(gdp_growth_pct - LAG(gdp_growth_pct)
         OVER (PARTITION BY country_code ORDER BY year), 2) AS yoy_change
FROM `your_project.imf_economic_analytics.weo_enriched`
WHERE country_code IN ('IDN','USA','CHN')
  AND year BETWEEN 2020 AND 2027
ORDER BY country_name, year;
