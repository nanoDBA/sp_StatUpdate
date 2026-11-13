-- =============================================
-- Multi-Version Test Database Setup
-- =============================================
-- Self-contained base database for version gate testing.
-- Compatible with SQL 2017, 2019, 2022.
-- Rebuild from a clean fixture state on every gate run.
-- =============================================

USE [master];
GO

-- =============================================
-- CommandLog in master (where sp_StatUpdate lives)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'CommandLog')
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        DatabaseName NVARCHAR(128),
        SchemaName NVARCHAR(128),
        ObjectName NVARCHAR(128),
        ObjectType NVARCHAR(50),
        IndexName NVARCHAR(128),
        IndexType TINYINT,
        StatisticsName NVARCHAR(128),
        PartitionNumber INT,
        ExtendedInfo XML,
        Command NVARCHAR(MAX),
        CommandType NVARCHAR(60),
        StartTime DATETIME2,
        EndTime DATETIME2,
        ErrorNumber INT,
        ErrorMessage NVARCHAR(MAX)
    );
END
GO

-- Each gate iteration starts without log history from a previous suite.
DELETE FROM dbo.CommandLog;
GO

-- tracking issue: ensure the CommandType-leading index exists even if CommandLog
-- pre-existed (e.g. created without indexes by diag/Setup-DiagTestData.sql earlier in the
-- session).  Created idempotently OUTSIDE the table guard so the proc never emits the
-- CommandLog index advisory (#31, sp_StatUpdate.sql ~line 1233) in test envs.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'IX_CommandLog_CommandType_StartTime')
    CREATE NONCLUSTERED INDEX IX_CommandLog_CommandType_StartTime ON dbo.CommandLog (CommandType, StartTime) INCLUDE (DatabaseName, SchemaName, ObjectName, StatisticsName, EndTime, ErrorNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'IX_CommandLog_StartTime')
    CREATE NONCLUSTERED INDEX IX_CommandLog_StartTime ON dbo.CommandLog (StartTime) INCLUDE (CommandType, EndTime);
GO

IF DB_ID(N'StatUpdateTest') IS NOT NULL
BEGIN
    ALTER DATABASE [StatUpdateTest] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [StatUpdateTest];
END
CREATE DATABASE [StatUpdateTest];
GO

USE [StatUpdateTest];
GO

-- =============================================
-- CommandLog table also in test DB (for reference)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'CommandLog')
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        DatabaseName NVARCHAR(128),
        SchemaName NVARCHAR(128),
        ObjectName NVARCHAR(128),
        ObjectType NVARCHAR(50),
        IndexName NVARCHAR(128),
        IndexType TINYINT,
        StatisticsName NVARCHAR(128),
        PartitionNumber INT,
        ExtendedInfo XML,
        Command NVARCHAR(MAX),
        CommandType NVARCHAR(60),
        StartTime DATETIME2,
        EndTime DATETIME2,
        ErrorNumber INT,
        ErrorMessage NVARCHAR(MAX)
    );
END
GO

-- tracking issue: ensure the CommandType-leading index exists even if CommandLog
-- pre-existed.  Created idempotently OUTSIDE the table guard (matches advisory #31).
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'IX_CommandLog_CommandType_StartTime')
    CREATE NONCLUSTERED INDEX IX_CommandLog_CommandType_StartTime ON dbo.CommandLog (CommandType, StartTime) INCLUDE (DatabaseName, SchemaName, ObjectName, StatisticsName, EndTime, ErrorNumber);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'IX_CommandLog_StartTime')
    CREATE NONCLUSTERED INDEX IX_CommandLog_StartTime ON dbo.CommandLog (StartTime) INCLUDE (CommandType, EndTime);
GO

-- =============================================
-- Orders table (with NORECOMPUTE stats for T09)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Orders')
BEGIN
    CREATE TABLE dbo.Orders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        CustomerName NVARCHAR(100),
        OrderDate DATETIME2 DEFAULT SYSDATETIME(),
        Amount DECIMAL(10,2)
    );
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000
        N'Customer' + CAST(ABS(CHECKSUM(NEWID())) % 100 AS NVARCHAR(10)),
        ABS(CHECKSUM(NEWID())) % 10000 * 0.01
    FROM sys.all_columns a CROSS JOIN sys.all_columns b;

    UPDATE STATISTICS dbo.Orders WITH NORECOMPUTE;
END
GO

-- =============================================
-- Products table
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Products')
BEGIN
    CREATE TABLE dbo.Products (
        ProductID INT IDENTITY(1,1) PRIMARY KEY,
        ProductName NVARCHAR(100),
        Price DECIMAL(10,2)
    );
    INSERT INTO dbo.Products (ProductName, Price)
    SELECT TOP 2000
        N'Product' + CAST(ABS(CHECKSUM(NEWID())) % 500 AS NVARCHAR(10)),
        ABS(CHECKSUM(NEWID())) % 500 * 0.01
    FROM sys.all_columns;
END
GO

-- =============================================
-- NorecomputeDemo table (NORECOMPUTE statistics)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'NorecomputeDemo')
BEGIN
    CREATE TABLE dbo.NorecomputeDemo (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        StatusCode INT NOT NULL,
        Amount DECIMAL(18,2) NOT NULL,
        ProcessedDate DATETIME2,
        Notes NVARCHAR(500)
    );
    INSERT INTO dbo.NorecomputeDemo (StatusCode, Amount, ProcessedDate, Notes)
    SELECT TOP 5000
        ABS(CHECKSUM(NEWID())) % 5 + 1,
        ABS(CHECKSUM(NEWID())) % 10000 * 0.01,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()),
        N'Record ' + CAST(ABS(CHECKSUM(NEWID())) % 5000 AS NVARCHAR(10))
    FROM sys.all_columns;

    CREATE STATISTICS ST_NorecomputeDemo_StatusCode ON dbo.NorecomputeDemo(StatusCode) WITH NORECOMPUTE;
    CREATE STATISTICS ST_NorecomputeDemo_Amount ON dbo.NorecomputeDemo(Amount) WITH NORECOMPUTE;
END
GO

-- =============================================
-- ModificationDemo table (high mod counter)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'ModificationDemo')
BEGIN
    CREATE TABLE dbo.ModificationDemo (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        Region VARCHAR(50) NOT NULL,
        SalesAmount DECIMAL(18,2) NOT NULL,
        TransactionDate DATE NOT NULL,
        CustomerID INT
    );
    INSERT INTO dbo.ModificationDemo (Region, SalesAmount, TransactionDate, CustomerID)
    SELECT TOP 10000
        CASE ABS(CHECKSUM(NEWID())) % 5
            WHEN 0 THEN 'North' WHEN 1 THEN 'South' WHEN 2 THEN 'East'
            WHEN 3 THEN 'West' ELSE 'Central'
        END,
        ABS(CHECKSUM(NEWID())) % 5000 * 0.01,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 180, GETDATE()),
        ABS(CHECKSUM(NEWID())) % 1000 + 1
    FROM sys.all_objects;

    UPDATE STATISTICS dbo.ModificationDemo;
END
GO

-- =============================================
-- PartitionedSales (incremental stats for V4)
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_SalesYear')
BEGIN
    CREATE PARTITION FUNCTION pf_SalesYear (INT)
    AS RANGE RIGHT FOR VALUES (2020, 2021, 2022, 2023, 2024);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_SalesYear')
BEGIN
    CREATE PARTITION SCHEME ps_SalesYear
    AS PARTITION pf_SalesYear ALL TO ([PRIMARY]);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'PartitionedSales')
BEGIN
    CREATE TABLE dbo.PartitionedSales (
        SaleID INT IDENTITY(1,1),
        SaleYear INT NOT NULL,
        Amount DECIMAL(10,2) NOT NULL,
        Region NVARCHAR(50) NOT NULL,
        CONSTRAINT PK_PartitionedSales PRIMARY KEY CLUSTERED (SaleYear, SaleID)
    ) ON ps_SalesYear(SaleYear);

    -- Seed rows per partition
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 1000 2020, ABS(CHECKSUM(NEWID())) % 10000 * 0.01, N'North' FROM sys.all_columns;
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 1000 2021, ABS(CHECKSUM(NEWID())) % 10000 * 0.01, N'South' FROM sys.all_columns;
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 1000 2022, ABS(CHECKSUM(NEWID())) % 10000 * 0.01, N'East' FROM sys.all_columns;
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 1000 2023, ABS(CHECKSUM(NEWID())) % 10000 * 0.01, N'West' FROM sys.all_columns;
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 1000 2024, ABS(CHECKSUM(NEWID())) % 10000 * 0.01, N'Central' FROM sys.all_columns;

    CREATE STATISTICS ST_PartitionedSales_Amount ON dbo.PartitionedSales (Amount) WITH INCREMENTAL = ON;
    CREATE STATISTICS ST_PartitionedSales_Region ON dbo.PartitionedSales (Region) WITH INCREMENTAL = ON;
END
GO

RAISERROR(N'setup_multiversion_db.sql completed successfully.', 10, 1) WITH NOWAIT;
GO
