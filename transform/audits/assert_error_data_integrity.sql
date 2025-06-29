AUDIT (
    name assert_error_data_integrity
);

SELECT 
    COUNT(*) as total_errors,
    COUNT(CASE WHEN error_message IS NULL OR error_message = '' THEN 1 END) as null_error_messages,
    COUNT(CASE WHEN error_type IS NULL OR error_type = '' THEN 1 END) as null_error_types,
    COUNT(CASE WHEN quarantined_at IS NULL THEN 1 END) as null_quarantine_times,
    COUNT(CASE WHEN error_sequence IS NULL OR error_sequence <= 0 THEN 1 END) as invalid_error_sequences
FROM @this_model
WHERE error_message IS NULL 
   OR error_message = ''
   OR error_type IS NULL 
   OR error_type = ''
   OR quarantined_at IS NULL
   OR error_sequence IS NULL 
   OR error_sequence <= 0 