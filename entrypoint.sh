#!/bin/bash

# Start SQL Server
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to start
sleep 15

# Run database initialization script
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -d master -i /home/demi/demi/data-pipeline/source/db_init.sql

# Run ETL script
/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P $SA_PASSWORD -d CampaignBD -i /home/demi/demi/data-pipeline/source/etl.sql

# Move files from incoming to processed directory
mv /home/demi/demi/data-pipeline/data/incoming/* /home/demi/demi/data-pipeline/data/processed/

# Keep the container running
tail -f /dev/null
