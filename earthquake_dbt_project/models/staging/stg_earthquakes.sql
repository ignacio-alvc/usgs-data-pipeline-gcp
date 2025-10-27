{{
    config(
        materialized='incremental',
        unique_key='earthqueake_id',
        partition_by={
            "field": "event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by = ["magnitude_type", "tsunami_alert"]
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ source('raw_data', 'raw_usgs_earthquakes') }}
    {% if is_incremental() %}
    WHERE TIMESTAMP_MILLIS (properties.time) > (SELECT MAX(event_timestamp) FROM {{ this }})
    {% endif %}
),

renamed_deduplicated AS (
        SELECT
        id AS earthquakes_id,
        properties.mag AS magnitude,
        properties.place AS place,
        TIMESTAMP_MILLIS(properties.time) AS event_timestamp, 
        properties.url AS url,
        properties.status AS status,
        properties.tsunami AS tsunami_alert,
        properties.magType AS magnitude_type,
        geometry.coordinates[OFFSET(0)] AS longitude,
        geometry.coordinates[OFFSET(1)] AS latitude,
        geometry.coordinates[OFFSET(2)] AS depth_km
        TIMESTAMP_MILLIS(properties.time) AS unique_sort_key
        _load_timestamp AS unique_sort_key
    FROM source_data
    WHERE properties.mag IS NOT NULL AND id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY earthquake_id ORDER BY unique_sort_key DESC) = 1
)

SELECT
    rd.earthquake_id,
    rd.magnitude,
    rd.place,
    rd.event_timestamp,
    rd.url,
    rd.status,
    rd.tsunami_alert,
    CASE
        WHEN rd.tsunami_alert = 1 THEN 'Si'
        ELSE ' No'
    END AS tsunami_alert_text,
    rd.magnitude_type,
    rd.longitude,
    rd.latitude,
    rd.depth_km,

    EXTRACT(DAYOFWEEK FROM rd.event_timestamp) AS day_of_week,
    FORMAT_TIMESTAMP('%A', rd.event_timestamp) AS day_name,
    EXTRACT(HOUR FROM rd.event_timestamp) AS hour_of_day,
    EXTRACT(MONTH FROM rd.event_timestamp) AS month_of_year,
    FORMAT_TIMESTAMP('%B', rd.event_timestamp) AS month_name,

    CASE
        WHEN rd.depth_km < 70 THEN 'Superficial'
        WHEN rd.depth_km <= 300 THEN 'Intermedio'
        ELSE 'Profundo'
    END AS depth_category

FROM renamed_deduplicated rd


