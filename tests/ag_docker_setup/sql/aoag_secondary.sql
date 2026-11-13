-- Secondary Replica Setup for Always On Availability Group
-- This script runs on db2 (secondary node)
-- Runs 20 seconds after primary to ensure AG is created first

USE [master];
GO

-- Create login for AG endpoint authentication (must match primary)
CREATE LOGIN aoag_login WITH PASSWORD = '$(AG_TEST_PASSWORD)';
CREATE USER aoag_user FOR LOGIN aoag_login;
GO

-- Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(AG_TEST_PASSWORD)';
GO

-- Import certificate from primary (via shared volume)
CREATE CERTIFICATE aoag_certificate
    AUTHORIZATION aoag_user
    FROM FILE = '/var/opt/mssql/shared/aoag_certificate.cert'
    WITH PRIVATE KEY (
        FILE = '/var/opt/mssql/shared/aoag_certificate.key',
        DECRYPTION BY PASSWORD = '$(AG_TEST_PASSWORD)'
    );
GO

-- Create HADR endpoint (must match primary configuration)
CREATE ENDPOINT [Hadr_endpoint]
    STATE = STARTED
    AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL)
    FOR DATA_MIRRORING (
        ROLE = ALL,
        AUTHENTICATION = CERTIFICATE aoag_certificate,
        ENCRYPTION = REQUIRED ALGORITHM AES
    );
GO

GRANT CONNECT ON ENDPOINT::[Hadr_endpoint] TO [aoag_login];
GO

-- Join the Availability Group created by primary
ALTER AVAILABILITY GROUP [AG1] JOIN WITH (CLUSTER_TYPE = NONE);
GO

-- Allow automatic seeding of databases
ALTER AVAILABILITY GROUP [AG1] GRANT CREATE ANY DATABASE;
GO

PRINT 'Secondary replica setup complete. Database will be seeded automatically.';
GO
