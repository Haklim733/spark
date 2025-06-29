AUDIT (
    name assert_valid_dates
);

SELECT 
    COUNT(*) as invalid_dates
FROM @this_model
WHERE generation_date IS NULL
   OR generation_date < '1900-01-01'
   OR generation_date > CURRENT_DATE + INTERVAL 1 DAY 