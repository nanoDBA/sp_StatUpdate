-- Primary Replica Setup for Always On Availability Group
-- This script runs on db1 (primary node)

USE [master];
GO

-- Create test database for AG
CREATE DATABASE Sales;
GO

USE [Sales];
GO

CREATE TABLE CUSTOMER (
    CustomerID INT NOT NULL PRIMARY KEY,
    CustomerName NVARCHAR(100),
    SalesAmount DECIMAL(18,2) NOT NULL
);
GO

INSERT INTO CUSTOMER (CustomerID, CustomerName, SalesAmount)
VALUES (1, 'Contoso', 100.00),
       (2, 'Fabrikam', 200.00),
       (3, 'Northwind', 300.00);
GO

-- Set recovery model to FULL (required for AG)
ALTER DATABASE [Sales] SET RECOVERY FULL;
GO

-- Take a full backup (required for AG seeding)
BACKUP DATABASE [Sales]
TO DISK = N'/var/opt/mssql/backup/Sales.bak'
WITH NOFORMAT, NOINIT,
     NAME = N'Sales-Full Database Backup',
     SKIP, NOREWIND, NOUNLOAD, STATS = 10;
GO

USE [master];
GO

-- Create login for AG endpoint authentication
CREATE LOGIN aoag_login WITH PASSWORD = '$(AG_TEST_PASSWORD)';
CREATE USER aoag_user FOR LOGIN aoag_login;
GO

-- Create master key and certificate for endpoint encryption
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$(AG_TEST_PASSWORD)';
GO

CREATE CERTIFICATE aoag_certificate
WITH SUBJECT = 'AG Authentication Certificate';
GO

-- Export certificate for secondary replica
BACKUP CERTIFICATE aoag_certificate
TO FILE = '/var/opt/mssql/shared/aoag_certificate.cert'
WITH PRIVATE KEY (
    FILE = '/var/opt/mssql/shared/aoag_certificate.key',
    ENCRYPTION BY PASSWORD = '$(AG_TEST_PASSWORD)'
);
GO

-- Create HADR endpoint
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

-- Create the Availability Group
-- Use dynamic SQL to get @@SERVERNAME for the primary replica name
DECLARE @cmd NVARCHAR(MAX);
SET @cmd = N'
CREATE AVAILABILITY GROUP [AG1]
WITH (CLUSTER_TYPE = NONE)
FOR REPLICA ON
    N''' + @@SERVERNAME + N''' WITH (
        ENDPOINT_URL = N''tcp://' + @@SERVERNAME + N':5022'',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        SEEDING_MODE = AUTOMATIC,
        FAILOVER_MODE = MANUAL,
        SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)
    ),
    N''db2'' WITH (
        ENDPOINT_URL = N''tcp://db2:5022'',
        AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
        SEEDING_MODE = AUTOMATIC,
        FAILOVER_MODE = MANUAL,
        SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)
    );';

EXEC sp_executesql @cmd;
GO

-- Wait for AG to be ready, then add database
WAITFOR DELAY '00:00:10';

ALTER AVAILABILITY GROUP [AG1] ADD DATABASE [Sales];
GO

PRINT 'Primary replica setup complete.';
GO
