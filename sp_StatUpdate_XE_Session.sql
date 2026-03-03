/*
sp_StatUpdate Extended Events Troubleshooting Session

Purpose:    Monitor sp_StatUpdate execution for troubleshooting.
            Captures both statement START and COMPLETION events for before/after
            correlation, plus wait stats, errors, and blocking.

Usage:
    1. Run this script to create the XE session
    2. Start the session before running sp_StatUpdate
    3. Review events after completion or during execution

To start:   ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;
To stop:    ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = STOP;
To drop:    DROP EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER;
To view:    See queries at bottom of this script

Created: 2026-01-28 for sp_StatUpdate troubleshooting (#8)
Updated: 2026-02-12 v2.0 - Added starting events for during-execution visibility
*/

-- Drop existing session if present
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'sp_StatUpdate_Monitor')
BEGIN
    DROP EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER;
END;
GO

-- Create the XE session
CREATE EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER

-- Capture UPDATE STATISTICS command START (for during-execution visibility)
ADD EVENT sqlserver.sp_statement_starting
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text)
    WHERE (
        sqlserver.like_i_sql_unicode_string(sqlserver.sql_text, N'%UPDATE STATISTICS%')
    )
),

-- Capture UPDATE STATISTICS command COMPLETION (duration, CPU, reads)
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
        OR error_number = 1222  -- Lock timeout
        OR error_number = 1205  -- Deadlock victim
        OR error_number = 3621  -- Statement aborted
        OR error_number = 8115  -- Arithmetic overflow
    )
),

-- Capture wait statistics for blocking/performance issues
-- Note: XE predicates require numeric map_key values for wait_type
-- Query sys.dm_xe_map_values WHERE name = 'wait_types' to find values
ADD EVENT sqlos.wait_completed
(
    ACTION (sqlserver.session_id, sqlserver.database_name)
    WHERE (
        duration > 1000000  -- > 1 second (in microseconds)
        AND (
               wait_type = 1    -- LCK_M_SCH_S (schema stability lock)
            OR wait_type = 2    -- LCK_M_SCH_M (schema modification lock)
            OR wait_type = 4    -- LCK_M_U (update lock)
            OR wait_type = 5    -- LCK_M_X (exclusive lock)
            OR wait_type = 66   -- PAGEIOLATCH_SH (shared page I/O latch)
            OR wait_type = 68   -- PAGEIOLATCH_EX (exclusive page I/O latch)
            OR wait_type = 281  -- CXPACKET (parallel query waits)
            OR wait_type = 187  -- NETWORK_IO (client waiting)
        )
    )
),

-- Capture long-running statement START (> 0 filter = all, correlate with completed)
ADD EVENT sqlserver.sql_statement_starting
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text)
    WHERE (
        sqlserver.like_i_sql_unicode_string(sqlserver.sql_text, N'%UPDATE STATISTICS%')
    )
),

-- Capture long-running statement COMPLETION (> 10 seconds)
ADD EVENT sqlserver.sql_statement_completed
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text, sqlserver.plan_handle)
    WHERE duration > 10000000  -- > 10 seconds (in microseconds)
),

-- Capture lock escalation (common during large stat scans)
ADD EVENT sqlserver.lock_escalation
(
    ACTION (sqlserver.session_id, sqlserver.database_name, sqlserver.sql_text)
),

-- Capture query timeouts and cancellations (attention = client cancel or timeout)
ADD EVENT sqlserver.attention
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
    SET max_memory = 8192  -- 8 MB ring buffer (increased for starting + completed events)
)

WITH (
    MAX_MEMORY = 8192 KB,
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

-- 5. Correlate starting/completed events (calculate in-progress duration)
;WITH ring_buffer AS
(
    SELECT
        CAST(target_data AS xml) AS event_data
    FROM sys.dm_xe_session_targets AS xst
    JOIN sys.dm_xe_sessions AS xs ON xs.address = xst.event_session_address
    WHERE xs.name = N'sp_StatUpdate_Monitor'
    AND   xst.target_name = N'ring_buffer'
),
events AS (
    SELECT
        event_data.value('(event/@name)[1]', 'varchar(50)') AS event_name,
        event_data.value('(event/@timestamp)[1]', 'datetime2(3)') AS event_time,
        event_data.value('(event/action[@name="session_id"]/value)[1]', 'int') AS session_id,
        LEFT(event_data.value('(event/action[@name="sql_text"]/value)[1]', 'nvarchar(max)'), 200) AS sql_text
    FROM ring_buffer
    CROSS APPLY event_data.nodes('RingBufferTarget/event') AS n(event_data)
    WHERE event_data.value('(event/@name)[1]', 'varchar(50)') IN ('sp_statement_starting', 'sp_statement_completed')
)
SELECT
    s.event_time AS start_time,
    c.event_time AS end_time,
    DATEDIFF(MILLISECOND, s.event_time, c.event_time) AS duration_ms,
    s.session_id,
    s.sql_text
FROM events AS s
LEFT JOIN events AS c ON c.session_id = s.session_id
    AND c.event_name = 'sp_statement_completed'
    AND c.event_time >= s.event_time
    AND c.sql_text = s.sql_text
WHERE s.event_name = 'sp_statement_starting'
ORDER BY s.event_time DESC;

-- 6. Export to file (optional - creates file in SQL Server default backup dir)
-- ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER
-- ADD TARGET package0.event_file (SET filename = N'sp_StatUpdate_Monitor.xel', max_file_size = 50);

===============================================================================
*/
