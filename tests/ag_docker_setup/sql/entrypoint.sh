#!/bin/bash

# Set default backup directory
/opt/mssql/bin/mssql-conf set filelocation.defaultbackupdir /var/opt/mssql/backup &

# Run initialization script in background
sh ./db-init.sh &

# Start SQL Server (foreground)
/opt/mssql/bin/sqlservr
