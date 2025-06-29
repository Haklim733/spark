MODEL (
    name nyc_taxi_aggregated,
    kind incremental,
    grain [date, pickup_location, dropoff_location]
);

SELECT 
    DATE(tpep_pickup_datetime) as trip_date,
    PULocationID as pickup_location,
    DOLocationID as dropoff_location,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance,
    AVG(fare_amount) as avg_fare,
    AVG(total_amount) as avg_total,
    SUM(total_amount) as total_revenue,
    AVG(passenger_count) as avg_passengers,
    MIN(tpep_pickup_datetime) as first_trip,
    MAX(tpep_pickup_datetime) as last_trip,
    CURRENT_TIMESTAMP() as processed_at
FROM nyc_taxi_data.yellow_tripdata
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND trip_distance > 0
  AND fare_amount > 0
GROUP BY 
    DATE(tpep_pickup_datetime),
    PULocationID,
    DOLocationID 