-- =============================================
-- sp_StatUpdate Test Database Setup
-- =============================================
-- Creates a test database with:
-- 1. Tables with skewed data distribution
-- 2. Statistics with NORECOMPUTE flag
-- 3. Modification counter scenarios
-- 4. CommandLog table for sp_StatUpdate logging
-- =============================================

USE [master];
GO

-- Drop existing test database if exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'StatUpdateTest')
BEGIN
    ALTER DATABASE [StatUpdateTest] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [StatUpdateTest];
END
GO

-- Create test database
CREATE DATABASE [StatUpdateTest];
GO

USE [StatUpdateTest];
GO

-- =============================================
-- Create CommandLog table (required by sp_StatUpdate)
-- =============================================
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
GO

CREATE INDEX IX_CommandLog_CommandType ON dbo.CommandLog(CommandType) INCLUDE (StartTime, EndTime);
CREATE INDEX IX_CommandLog_StartTime ON dbo.CommandLog(StartTime) INCLUDE (CommandType, EndTime);
GO

-- =============================================
-- Table 1: SkewedData - Highly skewed distribution
-- =============================================
-- CategoryID 1: 90% of rows (skewed)
-- CategoryID 2-10: 10% of rows (rare categories)
-- =============================================

PRINT 'Creating SkewedData table with highly skewed distribution...';

CREATE TABLE dbo.SkewedData (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT NOT NULL,
    Value DECIMAL(18,2) NOT NULL,
    Description NVARCHAR(100),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);
GO

-- Insert skewed data (90% in category 1)
SET NOCOUNT ON;

-- Category 1: 90,000 rows (majority)
INSERT INTO dbo.SkewedData (CategoryID, Value, Description)
SELECT
    1,
    RAND(CHECKSUM(NEWID())) * 1000,
    'Common category - ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10))
FROM sys.all_objects a
CROSS JOIN sys.all_objects b
WHERE ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) <= 90000;

-- Batch insert for other categories (1000 rows each)
DECLARE @cat INT = 2;
WHILE @cat <= 10
BEGIN
    INSERT INTO dbo.SkewedData (CategoryID, Value, Description)
    SELECT TOP 1000
        @cat,
        RAND(CHECKSUM(NEWID())) * 1000,
        'Rare category ' + CAST(@cat AS VARCHAR(2)) + ' - ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10))
    FROM sys.all_objects;

    SET @cat = @cat + 1;
END
GO

-- Create index and statistics on skewed column
CREATE NONCLUSTERED INDEX IX_SkewedData_CategoryID ON dbo.SkewedData(CategoryID) INCLUDE (Value);
GO

-- =============================================
-- Table 2: NorecomputeDemo - Statistics with NORECOMPUTE
-- =============================================
PRINT 'Creating NorecomputeDemo table with NORECOMPUTE statistics...';

CREATE TABLE dbo.NorecomputeDemo (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    StatusCode INT NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    ProcessedDate DATETIME2,
    Notes NVARCHAR(500)
);
GO

-- Insert data
INSERT INTO dbo.NorecomputeDemo (StatusCode, Amount, ProcessedDate, Notes)
SELECT TOP 50000
    ABS(CHECKSUM(NEWID())) % 5 + 1,
    RAND(CHECKSUM(NEWID())) * 10000,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()),
    'Record ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR(10))
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;
GO

-- Create statistics with NORECOMPUTE
CREATE STATISTICS ST_NorecomputeDemo_StatusCode ON dbo.NorecomputeDemo(StatusCode) WITH NORECOMPUTE;
CREATE STATISTICS ST_NorecomputeDemo_Amount ON dbo.NorecomputeDemo(Amount) WITH NORECOMPUTE;
CREATE STATISTICS ST_NorecomputeDemo_ProcessedDate ON dbo.NorecomputeDemo(ProcessedDate) WITH NORECOMPUTE;
GO

-- Create an index to get additional auto-created stats
CREATE NONCLUSTERED INDEX IX_NorecomputeDemo_StatusCode ON dbo.NorecomputeDemo(StatusCode) INCLUDE (Amount);
GO

-- Update the index stats with NORECOMPUTE to simulate production scenario
UPDATE STATISTICS dbo.NorecomputeDemo IX_NorecomputeDemo_StatusCode WITH NORECOMPUTE;
GO

-- =============================================
-- Table 3: ModificationDemo - High modification counter
-- =============================================
PRINT 'Creating ModificationDemo table for modification counter testing...';

CREATE TABLE dbo.ModificationDemo (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Region VARCHAR(50) NOT NULL,
    SalesAmount DECIMAL(18,2) NOT NULL,
    TransactionDate DATE NOT NULL,
    CustomerID INT
);
GO

-- Insert initial data
INSERT INTO dbo.ModificationDemo (Region, SalesAmount, TransactionDate, CustomerID)
SELECT TOP 10000
    CASE ABS(CHECKSUM(NEWID())) % 5
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        WHEN 3 THEN 'West'
        ELSE 'Central'
    END,
    RAND(CHECKSUM(NEWID())) * 5000,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 180, GETDATE()),
    ABS(CHECKSUM(NEWID())) % 1000 + 1
FROM sys.all_objects a;
GO

-- Create index and force stats update
CREATE NONCLUSTERED INDEX IX_ModificationDemo_Region ON dbo.ModificationDemo(Region) INCLUDE (SalesAmount);
GO

-- Update statistics to set baseline
UPDATE STATISTICS dbo.ModificationDemo;
GO

-- Now insert more rows to create modification counter > 0
-- This simulates data changes that haven't triggered auto-update
INSERT INTO dbo.ModificationDemo (Region, SalesAmount, TransactionDate, CustomerID)
SELECT TOP 3000
    'North',  -- Skew towards North
    RAND(CHECKSUM(NEWID())) * 5000,
    GETDATE(),
    ABS(CHECKSUM(NEWID())) % 1000 + 1
FROM sys.all_objects;
GO

-- =============================================
-- Table 4: RegularStats - Normal statistics (no NORECOMPUTE)
-- =============================================
PRINT 'Creating RegularStats table with normal auto-update statistics...';

CREATE TABLE dbo.RegularStats (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    ProductCode VARCHAR(20) NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    OrderDate DATE NOT NULL
);
GO

INSERT INTO dbo.RegularStats (ProductCode, Quantity, UnitPrice, OrderDate)
SELECT TOP 25000
    'PRD-' + RIGHT('00000' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR(5)), 5),
    ABS(CHECKSUM(NEWID())) % 100 + 1,
    RAND(CHECKSUM(NEWID())) * 500,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 90, GETDATE())
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;
GO

CREATE NONCLUSTERED INDEX IX_RegularStats_ProductCode ON dbo.RegularStats(ProductCode);
CREATE NONCLUSTERED INDEX IX_RegularStats_OrderDate ON dbo.RegularStats(OrderDate) INCLUDE (Quantity, UnitPrice);
GO

-- =============================================
-- Table 5: MixedStats - Some NORECOMPUTE, some regular
-- =============================================
PRINT 'Creating MixedStats table with mixed statistics types...';

CREATE TABLE dbo.MixedStats (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Department VARCHAR(50) NOT NULL,
    EmployeeCount INT NOT NULL,
    Budget DECIMAL(18,2) NOT NULL,
    FiscalYear INT NOT NULL
);
GO

INSERT INTO dbo.MixedStats (Department, EmployeeCount, Budget, FiscalYear)
SELECT TOP 15000
    CASE ABS(CHECKSUM(NEWID())) % 8
        WHEN 0 THEN 'Engineering'
        WHEN 1 THEN 'Sales'
        WHEN 2 THEN 'Marketing'
        WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'HR'
        WHEN 5 THEN 'Operations'
        WHEN 6 THEN 'Legal'
        ELSE 'IT'
    END,
    ABS(CHECKSUM(NEWID())) % 500 + 10,
    RAND(CHECKSUM(NEWID())) * 1000000,
    2020 + ABS(CHECKSUM(NEWID())) % 7
FROM sys.all_objects a;
GO

-- Create regular statistics (auto-update)
CREATE STATISTICS ST_MixedStats_Department ON dbo.MixedStats(Department);
CREATE STATISTICS ST_MixedStats_FiscalYear ON dbo.MixedStats(FiscalYear);

-- Create NORECOMPUTE statistics
CREATE STATISTICS ST_MixedStats_Budget ON dbo.MixedStats(Budget) WITH NORECOMPUTE;
CREATE STATISTICS ST_MixedStats_EmployeeCount ON dbo.MixedStats(EmployeeCount) WITH NORECOMPUTE;
GO

CREATE NONCLUSTERED INDEX IX_MixedStats_Department ON dbo.MixedStats(Department) INCLUDE (Budget);
GO

-- =============================================
-- Summary: Show what was created
-- =============================================
PRINT '';
PRINT '=============================================';
PRINT 'Test Database Setup Complete';
PRINT '=============================================';
PRINT '';

-- Table summary
SELECT
    t.name AS TableName,
    SUM(p.rows) AS RowCount
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)  -- Heap or clustered index
  AND t.is_ms_shipped = 0
GROUP BY t.name
ORDER BY t.name;

PRINT '';
PRINT '=== Statistics Summary ===';

-- Statistics summary
SELECT
    OBJECT_NAME(s.object_id) AS TableName,
    s.name AS StatName,
    s.auto_created AS AutoCreated,
    s.no_recompute AS NoRecompute,
    sp.rows AS Rows,
    sp.modification_counter AS ModCounter
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
ORDER BY OBJECT_NAME(s.object_id), s.name;

PRINT '';
PRINT 'Setup completed successfully!';
GO
