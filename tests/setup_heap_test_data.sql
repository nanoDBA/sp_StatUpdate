-- Generate heap modifications to trigger statistics maintenance
USE TestDB;
GO

-- Check if LargeHeap exists, if not create it
IF OBJECT_ID('dbo.LargeHeap', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.LargeHeap (
        ID int NOT NULL,
        Value nvarchar(100),
        Category varchar(50),
        Amount decimal(10,2)
    );

    -- Insert test data
    INSERT INTO dbo.LargeHeap (ID, Value, Category, Amount)
    SELECT TOP 10000
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        N'TestValue' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS nvarchar(10)),
        'Category' + CAST(ABS(CHECKSUM(NEWID())) % 5 AS varchar(10)),
        ABS(CHECKSUM(NEWID())) % 10000 / 100.0
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b;

    -- Create statistics
    CREATE STATISTICS ST_LargeHeap_Value ON dbo.LargeHeap(Value);
    CREATE STATISTICS ST_LargeHeap_Category ON dbo.LargeHeap(Category);
END;
GO

-- Update existing rows to create modifications
UPDATE TOP (6000) dbo.LargeHeap
SET Amount = Amount + 1;
GO

-- Insert new rows
INSERT INTO dbo.LargeHeap (ID, Value, Category, Amount)
SELECT TOP 1000
    10000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    N'NewValue' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS nvarchar(10)),
    'NewCat',
    100
FROM sys.all_objects;
GO

RAISERROR(N'Heap test data ready - 7000+ modifications', 10, 1) WITH NOWAIT;
GO
