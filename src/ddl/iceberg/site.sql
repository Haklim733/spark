CREATE NAMESPACE IF NOT EXISTS energy;
CREATE OR REPLACE TABLE energy.pv_site (
    av_pressure FLOAT,
    av_temp FLOAT,
    climate_type STRING,
    elevation FLOAT,
    latitude DOUBLE,
    location STRING,
    longitude DOUBLE,
    public_name STRING,
    site_id STRING COMMENT 'Unique site identifier',
    system_id STRING COMMENT 'Unique system identifier',
    file_path STRING COMMENT 'File path for the source file',
    job_id STRING COMMENT 'Job identifier for operational tracking and debugging'
)
USING iceberg
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'comment' = 'Site master data'
);
ALTER TABLE energy.pv_site CREATE BRANCH IF NOT EXISTS main;