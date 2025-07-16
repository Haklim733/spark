CREATE NAMESPACE IF NOT EXISTS energy;

CREATE OR REPLACE TABLE energy.pv_system (
    area FLOAT,
    comments STRING,
    ended_on TIMESTAMP,
    power FLOAT,
    public_name STRING,
    site_id INT,
    started_on TIMESTAMP,
    system_id INT,
    file_path STRING COMMENT 'File path for the source file',
    job_id STRING COMMENT 'Job identifier for operational tracking and debugging'
)
USING iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'System master data'
);
ALTER TABLE energy.pv_system CREATE BRANCH IF NOT EXISTS main;