-- =============================================
-- Multi-Version Test Extras
-- =============================================
-- Idempotent: safe to re-run. Adds objects needed
-- by Test-VersionGates.ps1 that don't exist in
-- setup_test_database.sql.
--
-- Requires: StatUpdateTest database already exists
--           (run setup_test_database.sql first)
-- =============================================

USE [StatUpdateTest];
GO

-- =============================================
-- 1. PartitionedSales — incremental stats (V4)
-- =============================================
-- Partition function: yearly buckets
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_SalesYear')
BEGIN
    CREATE PARTITION FUNCTION pf_SalesYear (int)
    AS RANGE RIGHT FOR VALUES (2020, 2021, 2022, 2023, 2024);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_SalesYear')
BEGIN
    CREATE PARTITION SCHEME ps_SalesYear
    AS PARTITION pf_SalesYear ALL TO ([PRIMARY]);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PartitionedSales')
BEGIN
    CREATE TABLE dbo.PartitionedSales (
        SaleID      int IDENTITY(1,1),
        SaleYear    int NOT NULL,
        Amount      decimal(10,2) NOT NULL,
        Region      nvarchar(50) NOT NULL,
        CONSTRAINT PK_PartitionedSales PRIMARY KEY CLUSTERED (SaleYear, SaleID)
    ) ON ps_SalesYear(SaleYear);

    -- Seed ~1000 rows per partition (5 partitions)
    INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
    SELECT TOP 5000
        yr.SaleYear,
        ABS(CHECKSUM(NEWID())) % 10000 * 0.01,
        CASE ABS(CHECKSUM(NEWID())) % 4
            WHEN 0 THEN N'North'
            WHEN 1 THEN N'South'
            WHEN 2 THEN N'East'
            ELSE N'West'
        END
    FROM (VALUES (2020),(2021),(2022),(2023),(2024)) AS yr(SaleYear)
    CROSS APPLY (
        SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.all_columns a CROSS JOIN sys.all_columns b
    ) AS nums;

    -- Create incremental statistics
    CREATE STATISTICS ST_PartitionedSales_Amount
        ON dbo.PartitionedSales (Amount)
        WITH INCREMENTAL = ON;

    CREATE STATISTICS ST_PartitionedSales_Region
        ON dbo.PartitionedSales (Region)
        WITH INCREMENTAL = ON;
END
GO

-- =============================================
-- 2. Ensure NORECOMPUTE stat exists on Orders
--    (needed for V1 PERSIST_SAMPLE_PERCENT test)
-- =============================================
IF NOT EXISTS (
    SELECT 1 FROM sys.stats s
    WHERE s.object_id = OBJECT_ID('dbo.Orders')
    AND   s.name = 'ST_Orders_Amount_Norecompute'
)
BEGIN
    CREATE STATISTICS ST_Orders_Amount_Norecompute
        ON dbo.Orders (Amount)
        WITH NORECOMPUTE, FULLSCAN;
END
GO

-- =============================================
-- 3. Insert modifications so stats qualify
-- =============================================
INSERT INTO dbo.PartitionedSales (SaleYear, Amount, Region)
SELECT TOP 2000 2024, 999.99, N'Modified'
FROM sys.all_columns;
GO

INSERT INTO dbo.Orders (CustomerName, Amount)
SELECT TOP 2000 N'VersionGateTest', 77.77
FROM sys.all_columns;
GO

INSERT INTO dbo.Products (ProductName, Price)
SELECT TOP 1000 N'VersionGateTest', 33.33
FROM sys.all_columns;
GO

RAISERROR(N'setup_multiversion_extras.sql completed successfully.', 10, 1) WITH NOWAIT;
GO
