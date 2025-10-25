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
FROM {{ source('raw_data', 'raw_usgs_earthquakes') }}
WHERE properties.mag IS NOT NULL AND id IS NOT NULL
