-- sp_StatUpdate Test Data Setup
-- Run this in TestDB to create test tables and data

USE TestDB;
GO

-- Drop existing test tables
DROP TABLE IF EXISTS dbo.Orders;
DROP TABLE IF EXISTS dbo.Products;
DROP TABLE IF EXISTS dbo.Categories;
DROP TABLE IF EXISTS dbo.AuditLog;
DROP TABLE IF EXISTS dbo.Customers;
DROP TABLE IF EXISTS dbo.LargeHeap;
GO

-- Table 1: Large table with ascending key (ORDER BY tests)
CREATE TABLE dbo.Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    OrderDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    CustomerID INT NOT NULL,
    OrderTotal DECIMAL(18,2) NOT NULL,
    StatusCode CHAR(1) NOT NULL,
    Notes NVARCHAR(500)
);
GO

-- Table 2: Products with NORECOMPUTE statistic
CREATE TABLE dbo.Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    ProductName NVARCHAR(200) NOT NULL,
    CategoryID INT NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    InStock BIT NOT NULL DEFAULT 1
);
GO

-- Table 3: Small table (should be skipped with MinPageCount)
CREATE TABLE dbo.Categories (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    CategoryName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(500)
);
GO

-- Table 4: Skewed distribution table
CREATE TABLE dbo.AuditLog (
    LogID BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    LogDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    EventType NVARCHAR(50) NOT NULL,
    UserID INT,
    Details NVARCHAR(MAX)
);
GO

-- Table 5: Nullable columns table
CREATE TABLE dbo.Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    FirstName NVARCHAR(100) NOT NULL,
    LastName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(200),
    Phone NVARCHAR(20),
    City NVARCHAR(100),
    Country NVARCHAR(100)
);
GO

-- Table 6: Heap with no clustered index
CREATE TABLE dbo.LargeHeap (
    ID INT IDENTITY(1,1),
    Col1 NVARCHAR(100),
    Col2 NVARCHAR(100),
    Col3 NVARCHAR(100),
    Col4 DATETIME2 DEFAULT GETDATE(),
    Filler NCHAR(200)
);
GO

PRINT 'Tables created';
GO

-- ============================================================
-- Parallel queue infrastructure (required for @StatsInParallel tests)
-- Create here (before data inserts) to ensure it exists even if inserts fail.
-- dbo.QueueStatistic is auto-created by sp_StatUpdate when @StatsInParallel='Y'
-- ============================================================
IF OBJECT_ID(N'dbo.QueueStatistic') IS NOT NULL
BEGIN
    ALTER TABLE dbo.QueueStatistic DROP CONSTRAINT IF EXISTS FK_QueueStatistic_Queue;
    DROP TABLE dbo.QueueStatistic;
END;

IF OBJECT_ID(N'dbo.Queue') IS NOT NULL DROP TABLE dbo.Queue;

CREATE TABLE dbo.Queue (
    QueueID             int             NOT NULL IDENTITY(1,1),
    QueueType           nvarchar(50)    NOT NULL DEFAULT N'sp_StatUpdate',
    SchemaName          sysname         NOT NULL DEFAULT N'dbo',
    ObjectName          sysname         NOT NULL DEFAULT N'sp_StatUpdate',
    Parameters          nvarchar(max)   NULL,
    ParameterFingerprint int            NULL,
    /* Columns required by sp_StatUpdate parallel mode (v2.3+) */
    QueueStartTime      datetime2(3)    NULL,
    SessionID           smallint        NULL,
    RequestID           int             NULL,
    RequestStartTime    datetime2(3)    NULL,
    CONSTRAINT PK_Queue PRIMARY KEY CLUSTERED (QueueID)
);

PRINT 'Parallel queue infrastructure created (dbo.Queue ready for @StatsInParallel tests)';
GO

-- Populate Orders (5000 rows)
INSERT INTO dbo.Orders (OrderDate, CustomerID, OrderTotal, StatusCode, Notes)
SELECT TOP 5000
    DATEADD(MINUTE, -ROW_NUMBER() OVER (ORDER BY c1.object_id, c2.n), GETDATE()),
    ABS(CHECKSUM(NEWID())) % 1000 + 1,
    ABS(CHECKSUM(NEWID())) % 10000 / 100.0,
    CHAR(65 + ABS(CHECKSUM(NEWID())) % 5),
    'Test order note'
FROM sys.all_columns c1
CROSS JOIN (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY object_id) AS n FROM sys.all_columns) c2;
GO

PRINT 'Orders populated';
GO

-- Populate Products (1000 rows)
INSERT INTO dbo.Products (ProductName, CategoryID, Price, InStock)
SELECT TOP 1000
    'Product ' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    ABS(CHECKSUM(NEWID())) % 20 + 1,
    ABS(CHECKSUM(NEWID())) % 1000 / 10.0,
    ABS(CHECKSUM(NEWID())) % 2
FROM sys.all_columns;
GO

PRINT 'Products populated';
GO

-- Populate Categories (small table - 5 rows)
INSERT INTO dbo.Categories (CategoryName, Description)
VALUES
    ('Electronics', 'Electronic devices'),
    ('Clothing', 'Apparel items'),
    ('Books', 'Physical and digital books'),
    ('Home', 'Home items'),
    ('Sports', 'Sports equipment');
GO

PRINT 'Categories populated';
GO

-- Populate AuditLog (10000 rows, 90% PageView)
INSERT INTO dbo.AuditLog (LogDate, EventType, UserID, Details)
SELECT TOP 10000
    DATEADD(SECOND, -ROW_NUMBER() OVER (ORDER BY c1.object_id), GETDATE()),
    ISNULL(
        CASE
            WHEN ROW_NUMBER() OVER (ORDER BY c1.object_id) % 10 = 0
            THEN CHOOSE(ABS(CHECKSUM(NEWID())) % 5 + 1, 'Login', 'Logout', 'Purchase', 'Update', 'Delete')
            ELSE 'PageView'
        END,
        'PageView'
    ),
    ABS(CHECKSUM(NEWID())) % 5000 + 1,
    'Audit entry'
FROM sys.all_columns c1
CROSS JOIN (SELECT TOP 10 1 AS n FROM sys.all_columns) c2;
GO

PRINT 'AuditLog populated';
GO

-- Populate Customers (2000 rows with nullable values)
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, Country)
SELECT TOP 2000
    'First' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'Last' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    CASE WHEN ROW_NUMBER() OVER (ORDER BY object_id) % 5 = 0 THEN NULL
         ELSE 'email' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)) + '@test.com' END,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY object_id) % 7 = 0 THEN NULL
         ELSE '555-' + RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS NVARCHAR(10)), 4) END,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY object_id) % 4 = 0 THEN NULL
         ELSE 'City' + CAST(ABS(CHECKSUM(NEWID())) % 100 AS NVARCHAR(10)) END,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY object_id) % 9 = 0 THEN NULL ELSE 'USA' END
FROM sys.all_columns;
GO

PRINT 'Customers populated';
GO

-- Populate LargeHeap (3000 rows)
INSERT INTO dbo.LargeHeap (Col1, Col2, Col3, Filler)
SELECT TOP 3000
    'Value' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'Data' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'Info' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    REPLICATE('X', 200)
FROM sys.all_columns;
GO

PRINT 'LargeHeap populated';
GO

-- Create NORECOMPUTE statistic on Products.CategoryID
CREATE STATISTICS ST_Products_CategoryID ON dbo.Products(CategoryID) WITH FULLSCAN, NORECOMPUTE;
GO

-- Create various statistics for testing
CREATE STATISTICS ST_Orders_CustomerID ON dbo.Orders(CustomerID) WITH FULLSCAN;
CREATE STATISTICS ST_Orders_StatusCode ON dbo.Orders(StatusCode) WITH SAMPLE 50 PERCENT;
CREATE STATISTICS ST_AuditLog_EventType ON dbo.AuditLog(EventType) WITH FULLSCAN;
CREATE STATISTICS ST_Customers_Country ON dbo.Customers(Country) WITH SAMPLE 25 PERCENT;
GO

PRINT 'Statistics created';
GO

-- Summary
SELECT
    t.name AS TableName,
    SUM(p.rows) AS RowCount,
    SUM(a.total_pages) * 8 / 1024 AS TotalSpaceMB
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id
JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.name IN ('Orders', 'Products', 'Categories', 'AuditLog', 'Customers', 'LargeHeap')
GROUP BY t.name
ORDER BY RowCount DESC;
GO

PRINT 'Test data setup complete';
GO
