CREATE NAMESPACE IF NOT EXISTS energy;
CREATE OR REPLACE TABLE energy.pv_data (
    measured_on TIMESTAMP,
    utc_measured_on TIMESTAMP,
    metric_id STRING,
    value FLOAT,
    file_path STRING COMMENT 'Path to the file that contains the data',
    system_id INT,
    year INT,
    month INT,
    day INT,
    job_id STRING COMMENT 'Job identifier for operational tracking and debugging'
)
USING iceberg
PARTITIONED BY (year, month, day)
TBLPROPERTIES (
    'branch.enabled' = 'true',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    "write.wap.enabled" = 'true',
    'comment' = 'Photovoltaic site data partitioned by date'
); 

ALTER TABLE energy.pv_data CREATE BRANCH IF NOT EXISTS main;