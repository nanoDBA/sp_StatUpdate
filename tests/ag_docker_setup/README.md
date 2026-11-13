# SQL Server Always On Availability Groups - Docker Test Setup

Quick setup for testing `AVAILABILITY_GROUP_DATABASES` keyword in sp_StatUpdate.

## Overview

Creates a **clusterless (Read-Scale) Availability Group** with 2 SQL Server containers:
- `db1` (port 2500) - Primary replica
- `db2` (port 2600) - Secondary replica

This is sufficient to test `sys.dm_hadr_availability_replica_cluster_states` and the AG database selection logic.

## Files

```
ag_docker_setup/
├── docker-compose.yml      # Container orchestration
├── sql/
│   ├── Dockerfile          # SQL Server image with HADR enabled
│   ├── entrypoint.sh       # Container startup script
│   ├── db-init.sh          # Runs initialization SQL
│   ├── aoag_primary.sql    # Primary: create AG, endpoint, certificate
│   └── aoag_secondary.sql  # Secondary: join AG
└── README.md               # This file
```

## Quick Start

### Option 1: Run Full Test Suite (Recommended)

```powershell
cd tests/ag_docker_setup

# Start containers and run all tests
.\Test-AvailabilityGroupSupport.ps1 -StartContainers

# Or if containers already running:
.\Test-AvailabilityGroupSupport.ps1
```

### Option 2: Manual Setup

```powershell
cd tests/ag_docker_setup
docker-compose up --build -d

# Wait ~60 seconds for AG initialization
Start-Sleep -Seconds 60

# Verify AG is working
docker exec db1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$env:SPSTATUPDATE_TEST_SQL_PASSWORD" -C -Q "
SELECT ag.name AS AGName, ar.replica_server_name, ars.role_desc
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
LEFT JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id;
"
```

## Test sp_StatUpdate AG Support

```sql
-- Connect to db1 (primary) - should process AG databases
EXEC TestDB.dbo.sp_StatUpdate
    @Databases = N'AVAILABILITY_GROUP_DATABASES',
    @Execute = N'N',
    @Debug = 1;

-- Connect to db2 (secondary) - should exclude AG databases (read-only)
EXEC TestDB.dbo.sp_StatUpdate
    @Databases = N'AVAILABILITY_GROUP_DATABASES',
    @Execute = N'N',
    @Debug = 1;
```

## Cleanup

```powershell
docker-compose down -v
```

## Sources

- [cviorel.com - AlwaysOn AG Using Containers](https://www.cviorel.com/alwayson-availability-groups-using-containers/)
- [SQLServerCentral - SQL Server AlwaysOn with Docker Containers](https://www.sqlservercentral.com/articles/sql-server-alwayson-with-docker-containers)
- [SQL Table Talk - SQL Server Always On Environment Using Docker](https://www.sqltabletalk.com/?p=401)

## Notes

- This is a **clusterless AG** (CLUSTER_TYPE = NONE) - no Windows Failover Clustering needed
- Automatic seeding is used (no manual backup/restore required)
- Only manual failover is supported (no automatic failover)
- The `Sales` database is added to the AG for testing
