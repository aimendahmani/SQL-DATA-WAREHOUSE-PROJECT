-- =============================================
-- PostgreSQL Script to Create a Data Warehouse
-- This script:
-- 1. Checks if the 'datawarehouse' database exists.
-- 2. Drops it if it exists.
-- 3. Recreates the 'datawarehouse' database.
-- 4. Creates schemas: bronze, silver, and gold.
-- =============================================

-- Step 1: Drop the database if it exists
DO $$
DECLARE
    db_exists INTEGER;
BEGIN
    SELECT COUNT(*) INTO db_exists FROM pg_database WHERE datname = 'datawarehouse';
    IF db_exists > 0 THEN
        PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'datawarehouse';
        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END $$;

-- Step 2: Create the database
CREATE DATABASE datawarehouse;

-- Step 3: Connect to the database and create schemas
\c datawarehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS log;

-- Confirmation Message
RAISE NOTICE 'Database and schemas created successfully.';
