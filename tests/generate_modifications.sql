-- Generate modifications to trigger statistics updates
USE TestDB;
GO

PRINT '=== GENERATING MODIFICATIONS ===';
PRINT '';

-- 1. Modify Orders table (insert + update)
PRINT 'Modifying Orders table...';
INSERT INTO dbo.Orders (OrderDate, CustomerID, OrderTotal, StatusCode, Notes)
SELECT TOP 2000
    DATEADD(MINUTE, -ROW_NUMBER() OVER (ORDER BY c1.object_id), GETDATE()),
    ABS(CHECKSUM(NEWID())) % 1000 + 1,
    ABS(CHECKSUM(NEWID())) % 10000 / 100.0,
    CHAR(65 + ABS(CHECKSUM(NEWID())) % 5),
    'Modification test'
FROM sys.all_columns c1;

UPDATE TOP (500) dbo.Orders
SET StatusCode = CHAR(65 + ABS(CHECKSUM(NEWID())) % 5)
WHERE Notes = 'Modification test';
GO

-- 2. Modify Products table
PRINT 'Modifying Products table...';
INSERT INTO dbo.Products (ProductName, CategoryID, Price, InStock)
SELECT TOP 500
    'NewProduct ' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    ABS(CHECKSUM(NEWID())) % 20 + 1,
    ABS(CHECKSUM(NEWID())) % 1000 / 10.0,
    ABS(CHECKSUM(NEWID())) % 2
FROM sys.all_columns;
GO

-- 3. Modify AuditLog table (high volume)
PRINT 'Modifying AuditLog table...';
INSERT INTO dbo.AuditLog (LogDate, EventType, UserID, Details)
SELECT TOP 5000
    DATEADD(SECOND, -ROW_NUMBER() OVER (ORDER BY c1.object_id), GETDATE()),
    'NewEvent',
    ABS(CHECKSUM(NEWID())) % 5000 + 1,
    'Modification test entry'
FROM sys.all_columns c1;
GO

-- 4. Modify Customers table
PRINT 'Modifying Customers table...';
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, Country)
SELECT TOP 1000
    'NewFirst' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'NewLast' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'newemail' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)) + '@test.com',
    '555-' + RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS NVARCHAR(10)), 4),
    'NewCity' + CAST(ABS(CHECKSUM(NEWID())) % 50 AS NVARCHAR(10)),
    CASE WHEN ROW_NUMBER() OVER (ORDER BY object_id) % 3 = 0 THEN 'Canada' ELSE 'USA' END
FROM sys.all_columns;
GO

-- 5. Modify LargeHeap table
PRINT 'Modifying LargeHeap table...';
INSERT INTO dbo.LargeHeap (Col1, Col2, Col3, Filler)
SELECT TOP 2000
    'NewValue' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'NewData' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    'NewInfo' + CAST(ROW_NUMBER() OVER (ORDER BY object_id) AS NVARCHAR(10)),
    REPLICATE('Y', 200)
FROM sys.all_columns;
GO

PRINT '';
PRINT '=== MODIFICATION COUNTS ===';
SELECT
    OBJECT_NAME(s.object_id) AS TableName,
    s.name AS StatName,
    sp.modification_counter AS ModCount,
    sp.rows AS TotalRows,
    CAST(100.0 * sp.modification_counter / NULLIF(sp.rows, 0) AS decimal(5,1)) AS ModPercent,
    DATEDIFF(minute, sp.last_updated, GETDATE()) AS MinutesSinceUpdate
FROM sys.stats s
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
WHERE OBJECTPROPERTY(s.object_id, 'IsUserTable') = 1
AND sp.modification_counter > 0
ORDER BY sp.modification_counter DESC;
GO
