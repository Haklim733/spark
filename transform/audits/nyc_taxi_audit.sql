-- NYC Taxi Data Quality Audit using SQLMesh AUDIT syntax

-- 1. NYC Taxi Null Check
AUDIT (
  name assert_nyc_taxi_not_null,
);

SELECT COUNT(*) as null_count
FROM nyc_taxi_data.yellow_tripdata
WHERE VendorID IS NULL 
   OR tpep_pickup_datetime IS NULL 
   OR tpep_dropoff_datetime IS NULL;

-- 2. NYC Taxi Distance Validation
AUDIT (
  name assert_nyc_taxi_valid_distance,
);

SELECT COUNT(*) as invalid_distances
FROM nyc_taxi_data.yellow_tripdata
WHERE trip_distance <= 0 OR trip_distance > 100;

-- 3. NYC Taxi Fare Validation
AUDIT (
  name assert_nyc_taxi_valid_fare,
);
SELECT COUNT(*) as invalid_fares
FROM nyc_taxi_data.yellow_tripdata
WHERE fare_amount <= 0 OR fare_amount > 1000;

-- 4. NYC Taxi Date Validation
AUDIT (
  name assert_nyc_taxi_valid_dates,
);

SELECT COUNT(*) as future_trips
FROM nyc_taxi_data.yellow_tripdata
WHERE tpep_pickup_datetime > CURRENT_TIMESTAMP();

-- 5. NYC Taxi Passenger Count Validation
AUDIT (
  name assert_nyc_taxi_valid_passengers,
);

SELECT COUNT(*) as invalid_passengers
FROM nyc_taxi_data.yellow_tripdata
WHERE passenger_count <= 0 OR passenger_count > 10;

-- 6. NYC Taxi Trip Duration Validation
AUDIT (
  name assert_nyc_taxi_valid_duration,
);

SELECT COUNT(*) as invalid_durations
FROM nyc_taxi_data.yellow_tripdata
WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
   OR DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 1440; -- More than 24 hours

-- 7. NYC Taxi Location Validation
AUDIT (
  name assert_nyc_taxi_valid_locations,
);

SELECT COUNT(*) as invalid_locations
FROM nyc_taxi_data.yellow_tripdata
WHERE PULocationID IS NULL 
   OR DOLocationID IS NULL 
   OR PULocationID <= 0 
   OR DOLocationID <= 0;

-- 8. NYC Taxi Payment Type Validation
AUDIT (
  name assert_nyc_taxi_valid_payment,
);

SELECT COUNT(*) as invalid_payment_types
FROM nyc_taxi_data.yellow_tripdata
WHERE payment_type IS NULL 
   OR payment_type NOT IN (1, 2, 3, 4, 5, 6);

-- 9. NYC Taxi Rate Code Validation
AUDIT (
  name assert_nyc_taxi_valid_rate_code,
);

SELECT COUNT(*) as invalid_rate_codes
FROM nyc_taxi_data.yellow_tripdata
WHERE RatecodeID IS NULL 
   OR RatecodeID NOT IN (1, 2, 3, 4, 5, 6);

-- 10. NYC Taxi Store and Forward Validation
AUDIT (
  name assert_nyc_taxi_valid_store_fwd,
);

SELECT COUNT(*) as invalid_store_fwd
FROM nyc_taxi_data.yellow_tripdata
WHERE store_and_fwd_flag IS NULL 
   OR store_and_fwd_flag NOT IN ('Y', 'N'); 