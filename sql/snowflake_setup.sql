-- Snowflake bootstrap for the Developer Metrics Ingestion Platform.
-- Run as a role with sufficient privileges (ACCOUNTADMIN or SECURITYADMIN
-- for user/role management; SYSADMIN or ACCOUNTADMIN for database/warehouse
-- creation). Replace placeholders in <angle brackets>.

-- 1. Role first (referenced by the user's DEFAULT_ROLE below).
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS pipeline_role;

-- 2. Service user, with the RSA public key registered directly.
--    Generate the key pair locally first (see README step 3), then paste
--    the public key body here (no BEGIN/END lines, no line breaks).
CREATE USER IF NOT EXISTS pipeline_user
  RSA_PUBLIC_KEY = '<public-key-body>'
  DEFAULT_WAREHOUSE = '<YOUR_WAREHOUSE>'
  DEFAULT_ROLE = 'PIPELINE_ROLE'
  MUST_CHANGE_PASSWORD = FALSE;

-- If the user already exists, register/rotate the key instead:
-- ALTER USER pipeline_user SET RSA_PUBLIC_KEY = '<public-key-body>';

GRANT ROLE pipeline_role TO USER pipeline_user;

-- 3. Warehouse usage. List what you have with: SHOW WAREHOUSES;
GRANT USAGE ON WAREHOUSE <YOUR_WAREHOUSE> TO ROLE pipeline_role;

-- 4. Database and schema. Create them if they don't exist yet
--    (needs SYSADMIN or ACCOUNTADMIN).
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS dev_metrics_db;
CREATE SCHEMA IF NOT EXISTS dev_metrics_db.bronze;

USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE dev_metrics_db TO ROLE pipeline_role;
GRANT USAGE ON SCHEMA dev_metrics_db.bronze TO ROLE pipeline_role;
GRANT CREATE TABLE ON SCHEMA dev_metrics_db.bronze TO ROLE pipeline_role;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA dev_metrics_db.bronze TO ROLE pipeline_role;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA dev_metrics_db.bronze TO ROLE pipeline_role;

-- 5. Needed later for the dbt Silver/Gold layers, since dbt creates its
--    own schemas under the same database.
GRANT CREATE SCHEMA ON DATABASE dev_metrics_db TO ROLE pipeline_role;

-- 6. Verify.
DESCRIBE USER pipeline_user;            -- check RSA_PUBLIC_KEY_FP is populated
SHOW GRANTS TO ROLE pipeline_role;
SHOW GRANTS TO USER pipeline_user;      -- confirm PIPELINE_ROLE is listed