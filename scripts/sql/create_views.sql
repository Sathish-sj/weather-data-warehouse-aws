-- Athena Views for Fact Tables
-- Database: weather_dwh


-- Fact Weather Actual View
CREATE OR REPLACE VIEW fact_weather_actual AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cw.observation_time) AS actual_weather_key,
    dl.location_key,
    dd.date_key,
    cw.observation_time,
    cw.temperature_celsius,
    cw.feels_like_celsius,
    cw.temp_min_celsius,
    cw.temp_max_celsius,
    cw.humidity_percent,
    cw.pressure_hpa,
    cw.wind_speed_mps,
    cw.wind_direction_deg,
    cw.wind_gust_mps,
    cw.cloud_cover_percent,
    cw.visibility_meters,
    cw.weather_condition,
    cw.weather_description,
    cw.is_daytime,
    cw.heat_index_category,
    cw.batch_id,
    cw.processed_timestamp
FROM weather_dwh.current_weather cw
INNER JOIN weather_dwh.dim_location dl 
    ON cw.location_name = dl.location_name 
    AND cw.country_code = dl.country_code
    AND dl.is_current = true
INNER JOIN weather_dwh.dim_date dd 
    ON CAST(cw.observation_date AS DATE) = dd.full_date;
    

-- Fact Weather Forecast View
CREATE OR REPLACE VIEW fact_weather_forecast AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY fw.forecast_created_time, fw.forecast_for_time) AS forecast_key,
    dl.location_key,
    dd_created.date_key AS forecast_created_date_key,
    dd_forecast.date_key AS forecast_for_date_key,
    fw.forecast_created_time,
    fw.forecast_for_time,
    fw.forecast_horizon_hours,
    fw.temperature_celsius_forecast,
    fw.feels_like_celsius_forecast,
    fw.humidity_percent_forecast,
    fw.pressure_hpa_forecast,
    fw.wind_speed_mps_forecast,
    fw.wind_direction_deg_forecast,
    fw.cloud_cover_percent_forecast,
    fw.precipitation_probability,
    fw.weather_condition_forecast,
    fw.batch_id,
    fw.processed_timestamp
FROM weather_dwh.forecast_weather fw
INNER JOIN weather_dwh.dim_location dl 
    ON fw.location_name = dl.location_name 
    AND fw.country_code = dl.country_code
    AND dl.is_current = true
INNER JOIN weather_dwh.dim_date dd_created 
    ON CAST(fw.forecast_created_date AS DATE) = dd_created.full_date
INNER JOIN weather_dwh.dim_date dd_forecast 
    ON CAST(fw.forecast_for_date AS DATE) = dd_forecast.full_date;


-- Fact Forecast Accuracy View
CREATE OR REPLACE VIEW fact_forecast_accuracy AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY f.forecast_created_time, f.forecast_for_time) AS accuracy_key,
    f.location_key,
    f.forecast_created_date_key,
    f.forecast_for_date_key,
    f.forecast_horizon_hours,
    f.temperature_celsius_forecast,
    a.temperature_celsius AS temperature_celsius_actual,
    (f.temperature_celsius_forecast - a.temperature_celsius) AS temp_error_celsius,
    ABS(f.temperature_celsius_forecast - a.temperature_celsius) AS temp_absolute_error,
    f.humidity_percent_forecast,
    a.humidity_percent AS humidity_percent_actual,
    (f.humidity_percent_forecast - a.humidity_percent) AS humidity_error_percent,
    f.wind_speed_mps_forecast,
    a.wind_speed_mps AS wind_speed_mps_actual,
    f.weather_condition_forecast,
    a.weather_condition AS weather_condition_actual,
    CASE 
        WHEN f.weather_condition_forecast = a.weather_condition THEN true 
        ELSE false 
    END AS condition_match,
    f.forecast_created_time,
    f.forecast_for_time,
    a.observation_time AS actual_observation_time,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_timestamp,
    CASE 
        WHEN ABS(f.temperature_celsius_forecast - a.temperature_celsius) <= 1 THEN 'Excellent'
        WHEN ABS(f.temperature_celsius_forecast - a.temperature_celsius) <= 3 THEN 'Good'
        WHEN ABS(f.temperature_celsius_forecast - a.temperature_celsius) <= 5 THEN 'Fair'
        ELSE 'Poor'
    END AS temp_accuracy_category,
    CASE 
        WHEN ABS(f.temperature_celsius_forecast - a.temperature_celsius) <= 3 
             AND f.weather_condition_forecast = a.weather_condition 
        THEN true 
        ELSE false 
    END AS is_accurate_forecast
FROM weather_dwh.fact_weather_forecast f
INNER JOIN weather_dwh.fact_weather_actual a
    ON f.location_key = a.location_key
    AND ABS(
        TO_UNIXTIME(CAST(f.forecast_for_time AS TIMESTAMP)) - 
        TO_UNIXTIME(CAST(a.observation_time AS TIMESTAMP))
    ) < 3600;