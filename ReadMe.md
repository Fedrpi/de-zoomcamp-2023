# DE-ZOOMCAMP-2023

## Week 1: Ingest data to local Postgress
### How to:
1. Command ```docker-compose up``` will create two containers
   - Postgres db on host localhost:5432
   - pgAdmin to manage postgres on host localhost:80
2. Command ```docker build -t ingest_data .``` will use Dockerfile to create container with scripts that allows download and ingest data
3. Command ```docker run -it ingest_data``` will execute bash in container.
   
  To download file use next command
   ```
   python get_data.py <url to download file> \
                      <target path to save file> \
                      <file name prefix> \
                      --file-ext=<allow values 0 of 1
                                  0 for parquet extention
                                  1 for csv extention if csv is packet .gz if will be unpack>
   ```

   To ingest data to postgres use next command

   ```
   python ingest_data.py <path to folder with csv data to ingest> \
                         <db user> \
                         <db password> \
                         <db host> \
                         <db post> \
                         <db name> \
                         <db table name> \
                         <file name prefix>
   ```
### Homework 1: SQL queries
SQL queries file with homework 1 located [here](sql/ny_taxi.sql)