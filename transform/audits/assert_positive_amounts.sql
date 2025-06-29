AUDIT (
    name assert_positive_amounts
);

SELECT 
    COUNT(*) as invalid_amounts
FROM @this_model
WHERE fare_amount <= 0
   OR total_amount <= 0
   OR trip_distance <= 0
   OR tip_amount < 0
   OR tolls_amount < 0 