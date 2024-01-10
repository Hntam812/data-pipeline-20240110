# Use the official SQL Server 2019 image on Linux
FROM mcr.microsoft.com/mssql/server:2019-latest

# Create a directory to copy scripts
WORKDIR /usr/src/app

# Copy SQL Server scripts
COPY ./source/db_init.sql /usr/src/app/
COPY ./source/etl.sql /usr/src/app/

# Grant permissions for the scripts to execute
RUN chmod +x /usr/src/app/db_init.sql
RUN chmod +x /usr/src/app/etl.sql

# Create directories for data
RUN mkdir -p /usr/src/app/data/incoming
RUN mkdir -p /usr/src/app/data/output
RUN mkdir -p /usr/src/app/data/processed

# Copy entrypoint script
COPY ./entrypoint.sh /usr/src/app/
RUN chmod +x /usr/src/app/entrypoint.sh

# Set entry point
CMD /bin/bash ./entrypoint.sh
