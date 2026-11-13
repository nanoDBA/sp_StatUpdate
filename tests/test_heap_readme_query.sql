-- Test the full Heap Health Monitoring query from README.md
USE TestDB;
GO

RAISERROR(N'Testing Heap Health Monitoring query from README...', 10, 1) WITH NOWAIT;
GO

SELECT TOP 20
    DatabaseName,
    SchemaName,
    ObjectName,
    StatisticsName,
    StartTime,
    -- Heap health metrics
    ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') AS ForwardedRecords,
    ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint') AS [RowCount],
    -- Calculate forwarding ratio (high % = fragmented heap)
    CAST(
        ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
        NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0)
        AS decimal(5,2)
    ) AS ForwardingPct,
    ExtendedInfo.value('(/ExtendedInfo/SizeMB)[1]', 'int') AS SizeMB,
    -- Modification activity
    ExtendedInfo.value('(/ExtendedInfo/ModificationCounter)[1]', 'bigint') AS Modifications,
    ExtendedInfo.value('(/ExtendedInfo/ModificationPct)[1]', 'decimal(18,2)') AS ModificationPct,
    -- When last updated
    DATEDIFF(day, StartTime, GETDATE()) AS DaysAgo,
    -- Actionable recommendation
    CASE
        WHEN ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
             NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0) > 20
        THEN 'CRITICAL: Rebuild heap immediately'
        WHEN ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
             NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0) > 10
        THEN 'WARNING: Consider rebuild'
        ELSE 'OK'
    END AS Recommendation
FROM dbo.CommandLog
WHERE CommandType = 'UPDATE_STATISTICS'
AND ExtendedInfo.value('(/ExtendedInfo/IsHeap)[1]', 'bit') = 1
ORDER BY ID DESC;
GO

RAISERROR(N'✓ Query executed successfully!', 10, 1) WITH NOWAIT;
GO
