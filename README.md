# Data Pipeline Project

This project demonstrates a simple data pipeline using SQL Server. It includes scripts for initializing the database and performing ETL processes.

## Prerequisites

- Docker installed on your machine.

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/your_username/data-pipeline.git

2. Navigate to the project directory:

    cd data-pipeline

3. Build the Docker image:

    docker build -t my_data_pl.

4. Run the Docker container:

    docker run -d -p 1433:1433 --name my_data_pipeline_container my_data_pipeline

## Database Initialization
The database initialization script (db_init.sql) creates the necessary database and tables.

## ETL Process
The ETL process script (etl.sql) performs Extract, Transform, and Load operations from the staging area to the main Campaign table and captures changes in the Campaign_delta table.

## Accessing SQL Server
Server: localhost,1433
Username: SA
Password: your_password

## Running SQL Queries
You can run SQL queries using sqlcmd. For example:
    docker exec -it my_data_pipeline_container /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P your_password -d CampaignDB -Q "SELECT * FROM Campaign"

## Cleanup
To stop and remove the Docker container:

    docker stop my_data_pipeline_container
    docker rm my_data_pipeline_container

## Notes
Replace your_password with your desired SQL Server password.
Ensure that the necessary SQL scripts are correctly placed in the source directory.

