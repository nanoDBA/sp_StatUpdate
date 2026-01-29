/*
sp_StatUpdate Extended Events Troubleshooting Session

Purpose:    Monitor sp_StatUpdate execution for troubleshooting
            Captures query execution, wait stats, errors, and statement completion

Usage:
    1. Run this script to create the XE session
    2. Start the session before running sp_StatUpdate
    3. Review events after completion or during execution

To start:   ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;
To stop:    ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = STOP;
To drop:    DROP EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER;
To view:    See queries at bottom of this script

Created: 2026-01-28 for sp_StatUpdate troubleshooting (#8 Grant Fritchey feedback)
*/

-- Drop existing session if present
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'sp_StatUpdate_Monitor')
BEGIN
    DROP EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER;
END;
GO

-- Create the XE session
CREATE EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER

-- Capture UPDATE STATISTICS commands
ADD EVENT sqlserver.sp_statement_completed
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text, sqlserver.query_hash)
    WHERE (
        sqlserver.like_i_sql_unicode_string(sqlserver.sql_text, N'%UPDATE STATISTICS%')
        OR sqlserver.like_i_sql_unicode_string(sqlserver.sql_text, N'%sp_StatUpdate%')
    )
),

-- Capture errors during execution
ADD EVENT sqlserver.error_reported
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text)
    WHERE (
        severity >= 11
        OR error_number IN (1222, 1205, 3621, 8115) -- Lock timeout, deadlock, statement abort, overflow
    )
),

-- Capture wait statistics for blocking/performance issues
ADD EVENT sqlos.wait_completed
(
    ACTION (sqlserver.session_id, sqlserver.database_name)
    WHERE (
        -- Focus on common stat maintenance waits
        wait_type IN (
            N'LCK_M_SCH_S',      -- Schema stability lock (stat reads)
            N'LCK_M_SCH_M',      -- Schema modification lock (stat updates)
            N'LCK_M_X',          -- Exclusive lock
            N'LCK_M_U',          -- Update lock
            N'PAGEIOLATCH_SH',   -- Shared page I/O latch
            N'PAGEIOLATCH_EX',   -- Exclusive page I/O latch
            N'CXPACKET',         -- Parallel query waits
            N'ASYNC_NETWORK_IO'  -- Client waiting
        )
        AND duration > 1000000  -- > 1 second (in microseconds)
    )
),

-- Capture long-running statements (> 10 seconds)
ADD EVENT sqlserver.sql_statement_completed
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text, sqlserver.plan_handle)
    WHERE duration > 10000000  -- > 10 seconds (in microseconds)
),

-- Capture query timeouts
ADD EVENT sqlserver.query_canceled
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text)
),

-- Capture deadlocks involving stat maintenance
ADD EVENT sqlserver.xml_deadlock_report
(
    ACTION (sqlserver.session_id)
)

-- Output to ring buffer (in-memory, no file needed)
ADD TARGET package0.ring_buffer
(
    SET max_memory = 4096  -- 4 MB ring buffer
)

WITH (
    MAX_MEMORY = 4096 KB,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 5 SECONDS,
    STARTUP_STATE = OFF  -- Don't auto-start on SQL Server restart
);
GO

RAISERROR(N'XE session [sp_StatUpdate_Monitor] created.', 10, 1) WITH NOWAIT;
RAISERROR(N'To start: ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;', 10, 1) WITH NOWAIT;
GO

/*
===============================================================================
VIEWING XE DATA
===============================================================================

-- 1. Check session status
SELECT
    name,
    CASE WHEN ses.session_id IS NOT NULL THEN 'RUNNING' ELSE 'STOPPED' END AS status
FROM sys.server_event_sessions AS s
LEFT JOIN sys.dm_xe_sessions AS ses ON ses.name = s.name
WHERE s.name = N'sp_StatUpdate_Monitor';

-- 2. View captured events (while session is running)
;WITH ring_buffer AS
(
    SELECT
        CAST(target_data AS xml) AS event_data
    FROM sys.dm_xe_session_targets AS xst
    JOIN sys.dm_xe_sessions AS xs ON xs.address = xst.event_session_address
    WHERE xs.name = N'sp_StatUpdate_Monitor'
    AND   xst.target_name = N'ring_buffer'
)
SELECT
    event_data.value('(event/@name)[1]', 'varchar(50)') AS event_name,
    event_data.value('(event/@timestamp)[1]', 'datetime2(3)') AS event_time,
    event_data.value('(event/action[@name="session_id"]/value)[1]', 'int') AS session_id,
    event_data.value('(event/action[@name="database_name"]/value)[1]', 'sysname') AS database_name,
    event_data.value('(event/data[@name="duration"]/value)[1]', 'bigint') / 1000 AS duration_ms,
    event_data.value('(event/data[@name="cpu_time"]/value)[1]', 'bigint') / 1000 AS cpu_ms,
    event_data.value('(event/data[@name="logical_reads"]/value)[1]', 'bigint') AS logical_reads,
    LEFT(event_data.value('(event/action[@name="sql_text"]/value)[1]', 'nvarchar(max)'), 200) AS sql_text_truncated
FROM ring_buffer
CROSS APPLY event_data.nodes('RingBufferTarget/event') AS n(event_data)
ORDER BY event_time DESC;

-- 3. Summary by event type
;WITH ring_buffer AS
(
    SELECT
        CAST(target_data AS xml) AS event_data
    FROM sys.dm_xe_session_targets AS xst
    JOIN sys.dm_xe_sessions AS xs ON xs.address = xst.event_session_address
    WHERE xs.name = N'sp_StatUpdate_Monitor'
    AND   xst.target_name = N'ring_buffer'
)
SELECT
    event_data.value('(event/@name)[1]', 'varchar(50)') AS event_name,
    COUNT(*) AS event_count
FROM ring_buffer
CROSS APPLY event_data.nodes('RingBufferTarget/event') AS n(event_data)
GROUP BY event_data.value('(event/@name)[1]', 'varchar(50)')
ORDER BY event_count DESC;

-- 4. View wait statistics captured
;WITH ring_buffer AS
(
    SELECT
        CAST(target_data AS xml) AS event_data
    FROM sys.dm_xe_session_targets AS xst
    JOIN sys.dm_xe_sessions AS xs ON xs.address = xst.event_session_address
    WHERE xs.name = N'sp_StatUpdate_Monitor'
    AND   xst.target_name = N'ring_buffer'
)
SELECT
    event_data.value('(event/data[@name="wait_type"]/text)[1]', 'varchar(50)') AS wait_type,
    COUNT(*) AS wait_count,
    SUM(event_data.value('(event/data[@name="duration"]/value)[1]', 'bigint')) / 1000 AS total_duration_ms,
    AVG(event_data.value('(event/data[@name="duration"]/value)[1]', 'bigint')) / 1000 AS avg_duration_ms
FROM ring_buffer
CROSS APPLY event_data.nodes('RingBufferTarget/event') AS n(event_data)
WHERE event_data.value('(event/@name)[1]', 'varchar(50)') = 'wait_completed'
GROUP BY event_data.value('(event/data[@name="wait_type"]/text)[1]', 'varchar(50)')
ORDER BY total_duration_ms DESC;

-- 5. Export to file (optional - creates file in SQL Server default backup dir)
-- ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER
-- ADD TARGET package0.event_file (SET filename = N'sp_StatUpdate_Monitor.xel', max_file_size = 50);

===============================================================================
*/
