FROM python:3.9.1

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

WORKDIR /app
COPY /src/get_data.py get_data.py 
COPY /src/ingest_data.py ingest_data.py 

ENTRYPOINT [ "bash" ]

RUN python get_data.py https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet \
                       ./data/csv \
                       green_taxi_data \
                       --file-ext=1

# RUN python ingest_data.py ./data/csv \
#                           root \
#                           root \
#                           docker.for.mac.host.internal \
#                           5432 \
#                           ny_taxi \
#                           green_taxi_tripdata \
#                           green_tripdata
