AUDIT (
    name assert_valid_passenger_count
);

SELECT 
    COUNT(*) as invalid_passenger_counts
FROM @this_model
WHERE passenger_count IS NULL
   OR passenger_count < 0
   OR passenger_count > 10
   OR passenger_count != FLOOR(passenger_count) 