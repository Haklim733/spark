-- Test for null values in critical columns
SELECT COUNT(*) as null_count
FROM nyc_taxi_data.yellow_tripdata
WHERE VendorID IS NULL 
   OR tpep_pickup_datetime IS NULL 
   OR tpep_dropoff_datetime IS NULL;

-- Test for invalid trip distances
SELECT COUNT(*) as invalid_distances
FROM nyc_taxi_data.yellow_tripdata
WHERE trip_distance <= 0 OR trip_distance > 100;  -- Unrealistic distances

-- Test for invalid fare amounts
SELECT COUNT(*) as invalid_fares
FROM nyc_taxi_data.yellow_tripdata
WHERE fare_amount <= 0 OR fare_amount > 1000;  -- Unrealistic fares

-- Test for future dates
SELECT COUNT(*) as future_trips
FROM nyc_taxi_data.yellow_tripdata
WHERE tpep_pickup_datetime > CURRENT_TIMESTAMP();

-- Test for passenger count validity
SELECT COUNT(*) as invalid_passengers
FROM nyc_taxi_data.yellow_tripdata
WHERE passenger_count <= 0 OR passenger_count > 10;  -- Unrealistic passenger counts 