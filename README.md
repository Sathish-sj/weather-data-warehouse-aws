# Weather Data Warehouse on AWS

> End-to-end serverless data warehouse for weather forecast analysis across 8 global cities

![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![PySpark](https://img.shields.io/badge/PySpark-ETL-red)
![SQL](https://img.shields.io/badge/SQL-Athena-brightgreen)
## Project Overview

An automated data pipeline that extracts weather data from OpenWeather API, processes it through a medallion architecture (Bronze → Silver → Gold), and provides interactive dashboards for forecast accuracy analysis.

### Key Features

- **Automated Data Extraction**: Daily Lambda function pulls weather data for 8 cities
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (dimensional)
- **SCD Type 2**: Tracks historical changes in location dimensions
- **Star Schema**: Fact and dimension tables optimized for analytics
- **Real-time Dashboards**: QuickSight dashboards with auto-refresh
- **Forecast Analysis**: Evaluates prediction accuracy across multiple time horizons

---

## Architecture

[Project Architecture](architecture\architecture-diagram.png)

## Architecture Pattern

This project implements a **modern data lakehouse** - a cloud-native data 
warehouse architecture that combines:

- **Data Lake benefits:** Scalable S3 storage with open Parquet format
- **Data Warehouse capabilities:** Star schema, ACID transactions, SQL analytics

**Why Lakehouse over Traditional DW (Redshift)?**

For this use case (8 cities, daily updates, daily batch processing):
- **Cost:** $2/month vs $180/month minimum for Redshift
- **Elasticity:** S3 scales automatically vs manual cluster resizing  
- **Modern pattern:** Used by Netflix, Uber, Apple for similar workloads

**Lakehouse advantages for this use case:**
-  **93% cost savings** at current scale
-  **Elastic scaling** - costs grow linearly with data
-  **No idle costs** - pay only when querying
-  **Open format** - Parquet files portable across platforms**Lakehouse advantages for this use case:**

This demonstrates proficiency with modern data engineering patterns while 
maintaining core data warehouse principles (dimensional modeling, ETL, 
historical tracking).

### Data Flow

1. **Extract**: Lambda function runs daily at 6:00 AM UTC
2. **Bronze**: Raw JSON stored in S3
3. **Silver**: Glue job transforms to clean Parquet (6:30 AM)
4. **Gold**: Dimensions updated with SCD Type 2 (7:00 AM)
5. **Analytics**: Athena views join data, QuickSight refreshes (7:30 AM)

---

## Project Structure
```
weather-data-warehouse/
├── README.md
├── architecture/
│   └── architecture-diagram.png
├── scripts/
│   ├── lambda/
│   │   └── weather_extractor.py                  # API extraction logic
│   ├── glue/
│   │   ├── bronze_to_silver.py                   # Data cleaning & transformation
│   │   └── build_dimensions.py                   # SCD Type 2 dimension building
│   └── sql/
│       ├── create_views.sql                      # Athena view definitions
│       └── sample_queries.sql                    # Example analytics queries
├── dashboards/
│   ├── Weather Forecast Analytics Dashboard.pdf  # Forecast accuracy analysis
│   └── Weather Insights Dashboard.pdf            # Real-time weather monitoring
└── .gitignore
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Extraction** | AWS Lambda, Python 3.9 | API calls to OpenWeather |
| **Storage** | Amazon S3 | Data lake (Bronze, Silver, Gold) |
| **ETL** | AWS Glue, PySpark | Data transformation |
| **Orchestration** | Amazon EventBridge | Job scheduling |
| **Data Catalog** | AWS Glue Crawler | Schema detection |
| **Query Engine** | Amazon Athena | SQL analytics |
| **Visualization** | Amazon QuickSight | Interactive dashboards |
| **Monitoring** | CloudWatch Logs | Pipeline observability |

---

## Data Model

### Star Schema Design
```
                    ┌──────────────┐
                    │  dim_date    │
                    │              │
                    │ date_key (PK)│
                    │ full_date    │
                    │ year, month  │
                    │ day_name     │
                    └──────┬───────┘
                           │
        ┌─────────────┐    │    ┌─────────────────────┐
        │ dim_location│◄───┼───►│  fact_weather_actual│
        │             │    │    │                     │
        │location_key │    │    │ location_key (FK)   │
        │location_name│    │    │ date_key (FK)       │
        │timezone     │    │    │ temperature_celsius │
        │SCD Type 2   │    │    │ humidity_percent    │
        └─────────────┘    │    │ weather_condition   │
                           │    └─────────────────────┘
                           │
                           │    ┌──────────────────────┐
                           ├───►│ fact_weather_forecast│
                           │    │                      │
                           │    │ forecast_horizon_hrs │
                           │    │ temperature_forecast │
                           │    └──────────────────────┘
                           │
                           │    ┌───────────────────────┐
                           └───►│ fact_forecast_accuracy│
                                │                       │
                                │ temp_absolute_error   │
                                │ is_accurate_forecast  │
                                └───────────────────────┘
```

### Fact Tables

- **fact_weather_actual**: Current weather observations
- **fact_weather_forecast**: 5-day weather predictions
- **fact_forecast_accuracy**: Comparison of forecasts vs actuals

### Dimensions

- **dim_location**: 8 cities with SCD Type 2 for timezone changes
- **dim_date**: Calendar dimension (2024-2026)

---

## Getting Started

### Prerequisites

- AWS Account
- OpenWeather API key (free tier)
- Basic knowledge of AWS services
- Python 3.9+

### Setup Instructions

#### 1. **Create S3 Buckets**
```bash
aws s3 mb s3://weather-dwh-bronze-<your-name>
aws s3 mb s3://weather-dwh-silver-<your-name>
aws s3 mb s3://weather-dwh-gold-<your-name>
aws s3 mb s3://weather-scripts-<your-name>
```

#### 2. **Deploy Lambda Function**

1. Create Lambda function: `weather-data-extractor`
2. Runtime: Python 3.9
3. Copy code from `scripts/lambda/weather_extractor.py`
4. Add environment variables:
   - `API_KEY`: Your OpenWeather API key
   - `BRONZE_BUCKET`: Your Bronze S3 bucket name
5. Set timeout: 5 minutes
6. Set memory: 512 MB

#### 3. **Create EventBridge Rule**
```bash
# Schedule: Daily at 6:00 AM UTC
cron(0 6 * * ? *)
```

Target: Lambda function `weather-data-extractor`

#### 4. **Create Glue Jobs**

**Job 1: bronze-to-silver**
- Type: Spark
- Glue version: 4.0
- Worker type: G.1X (2 workers)
- Script: `scripts/glue/bronze_to_silver.py`
- Schedule: Daily at 6:30 AM UTC

**Job 2: build-dimensions**
- Type: Spark
- Glue version: 4.0
- Worker type: G.1X (2 workers)
- Script: `scripts/glue/build_dimensions.py`
- Schedule: Daily at 7:00 AM UTC

#### 5. **Create Athena Views**

Run SQL from `scripts/sql/create_views.sql` in Athena console.

#### 6. **Setup QuickSight**

1. Sign up for QuickSight Standard Edition
2. Grant access to S3 buckets
3. Create Athena data source
4. Import dashboard definitions from `dashboards/`

---

## Key Metrics & Insights

### Forecast Accuracy Results

- **Overall Accuracy**: 46.7% (within 3°C and correct conditions)
- **Best Performance**: London (94.4%) - stable temperate climate
- **Challenging**: Mumbai (16.4%) - tropical monsoon variability

### Accuracy by Time Horizon

| Horizon | Accuracy | Avg Error |
|---------|----------|-----------|
| 0-24h   | 60.7%   | 2.08°C   |
| 24-48h  | 43.6%   | 2.46°C   |
| 48-72h  | 50.0%   | 2.65°C   |
| 72h+    | 62.1%   | 1.24°C   |

### Forecast Quality Distribution

- **Excellent** (≤1°C error): 586 forecasts (31.5%)
- **Good** (1-3°C error): 735 forecasts (39.5%)
- **Fair** (3-5°C error): 293 forecasts (15.8%)
- **Poor** (>5°C error): 20 forecasts (1.1%)

---

## Cost Estimation

| Service | Monthly Cost | Notes |
|---------|-------------|-------|
| Lambda | $0.00 | Free tier covers usage |
| S3 | $0.50 | ~5GB data storage |
| Glue | $5.00 | 2 jobs × 5 min/day |
| Athena | $1.00 | ~200 queries/month |
| QuickSight | $9.00 | Standard Edition |
| **Total** | **~$15/month** | Can be covered by AWS credits |

---

## Sample Queries

### Query 1: Top Performing Cities
```sql
SELECT 
    location_name,
    ROUND(AVG(CAST(is_accurate_forecast AS INT)) * 100, 1) AS accuracy_pct,
    COUNT(*) AS forecasts_analyzed
FROM fact_forecast_accuracy fa
JOIN dim_location dl ON fa.location_key = dl.location_key
WHERE dl.is_current = true
GROUP BY location_name
ORDER BY accuracy_pct DESC;
```

### Query 2: Accuracy Trend Over Time
```sql
SELECT 
    DATE(forecast_created_time) AS forecast_date,
    AVG(temp_absolute_error) AS avg_error,
    AVG(CAST(is_accurate_forecast AS INT)) * 100 AS accuracy_pct
FROM fact_forecast_accuracy
GROUP BY DATE(forecast_created_time)
ORDER BY forecast_date;
```

More queries in `scripts/sql/sample_queries.sql`

---

## Key Learnings

### Technical Challenges Solved

1. **SCD Type 2 Implementation**: Tracking timezone changes across cities
2. **Schema Evolution**: Handling Parquet type mismatches in incremental loads
3. **Data Quality**: Implementing heat index categories and weather condition validation
4. **Performance**: Optimizing Glue jobs to run under 5 minutes

### Best Practices Applied

- Medallion architecture for data quality layers
- Partitioning by date for query performance
- SPICE refresh scheduling for dashboard performance
- CloudWatch monitoring for pipeline observability

---

## Future Enhancements

- [ ] Add more cities (50+ global locations)
- [ ] Implement ML model for prediction improvement
- [ ] Add historical weather trends (3-year lookback)
- [ ] Create alerts for forecast accuracy drops
- [ ] Integrate with Slack for daily weather summaries
- [ ] Add cost optimization with S3 lifecycle policies

---

## Screenshots

### Forecast Analytics Dashboard
![Forecast Analytics](dashboards\images\Weather-Forecast-Analytics-Dashboard.jpg)


### Weather Insights Dashboard
![Weather Insights](dashboards\images\Weather-Insights-Dashboard.jpg)

---

## Author

**Sathish J**

- LinkedIn: [Sathish](https://www.linkedin.com/in/sathish-jayaseelan/)

---

## Project Stats

- **Cities Analyzed**: 8 global locations
- **Daily Forecasts**: 320 predictions/day
- **Forecast Horizon**: Up to 120 hours
- **Data Processed**: 3000+ forecast comparisons
- **Pipeline Uptime**: 99%+
- **Dashboard Refresh**: Daily at 7:30 AM UTC

---

**Star this repo if you found it helpful!**
```
Built on AWS | Data Engineering Portfolio Project