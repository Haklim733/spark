-- PostgreSQL Database Initialization Script
-- This script creates the necessary databases for the Spark/Iceberg setup

-- Create the SQLMesh state database
CREATE DATABASE sqlmesh;

-- Grant privileges to the admin user
GRANT ALL PRIVILEGES ON DATABASE sqlmesh TO admin;

-- Connect to sqlmesh database and create necessary extensions
\c sqlmesh;

-- Enable required extensions for SQLMesh
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Show created databases
\l

->

-- PostgreSQL Database Initialization Script
-- This script creates the necessary databases for the Spark/Iceberg setup

-- Create the SQLMesh state database
CREATE DATABASE sqlmesh;

-- Grant privileges to the admin user
GRANT ALL PRIVILEGES ON DATABASE sqlmesh TO admin;

-- Connect to sqlmesh database and create necessary extensions
\c sqlmesh;

-- Enable required extensions for SQLMesh
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Show created databases
\l