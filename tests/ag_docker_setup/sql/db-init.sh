#!/bin/bash

SLEEP_TIME=$INIT_WAIT
SQL_SCRIPT=$INIT_SCRIPT

echo "Sleeping for ${SLEEP_TIME} seconds to allow SQL Server to start..."
sleep ${SLEEP_TIME}

echo "####### Running setup script ${SQL_SCRIPT} #######"

# Primary cleans up old certificates
if [ "$SQL_SCRIPT" = "aoag_primary.sql" ]
then
    rm -f /var/opt/mssql/shared/aoag_certificate.key 2>/dev/null
    rm -f /var/opt/mssql/shared/aoag_certificate.cert 2>/dev/null
fi

# Run the initialization SQL
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d master -v AG_TEST_PASSWORD="$MSSQL_SA_PASSWORD" -i "$SQL_SCRIPT"

echo "####### AOAG script execution completed #######"
