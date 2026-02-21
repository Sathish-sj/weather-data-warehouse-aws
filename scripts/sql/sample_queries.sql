-- Sample Analytics Queries for Weather Data Warehouse

-- 1: Overall Forecast Accuracy by Time Horizon
SELECT 
    CASE 
        WHEN forecast_horizon_hours <= 24 THEN '0-24 hours'
        WHEN forecast_horizon_hours <= 48 THEN '24-48 hours'
        WHEN forecast_horizon_hours <= 72 THEN '48-72 hours'
        ELSE '72+ hours'
    END AS horizon_bucket,
    COUNT(*) AS total_forecasts,
    ROUND(AVG(temp_absolute_error), 2) AS avg_error_celsius,
    ROUND(AVG(CAST(is_accurate_forecast AS INT)) * 100, 1) AS accuracy_pct
FROM fact_forecast_accuracy
GROUP BY 1
ORDER BY 1;

-- 2: City Performance Ranking
SELECT 
    dl.location_name,
    dl.country_code,
    COUNT(*) AS total_forecasts,
    ROUND(AVG(fa.temp_absolute_error), 2) AS avg_error,
    ROUND(AVG(CAST(fa.is_accurate_forecast AS INT)) * 100, 1) AS accuracy_pct
FROM fact_forecast_accuracy fa
JOIN dim_location dl ON fa.location_key = dl.location_key
WHERE dl.is_current = true
GROUP BY dl.location_name, dl.country_code
ORDER BY accuracy_pct DESC;

-- 3: Current Weather Summary
SELECT 
    dl.location_name,
    fa.observation_time,
    fa.temperature_celsius,
    fa.weather_condition,
    fa.humidity_percent,
    fa.wind_speed_mps
FROM fact_weather_actual fa
JOIN dim_location dl ON fa.location_key = dl.location_key
WHERE dl.is_current = true
  AND DATE(fa.observation_time) = CURRENT_DATE
ORDER BY dl.location_name;

-- 4: Forecast Quality Distribution
SELECT 
    temp_accuracy_category,
    COUNT(*) AS forecast_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM fact_forecast_accuracy
GROUP BY temp_accuracy_category
ORDER BY 
    CASE temp_accuracy_category
        WHEN 'Excellent' THEN 1
        WHEN 'Good' THEN 2
        WHEN 'Fair' THEN 3
        WHEN 'Poor' THEN 4
    END;