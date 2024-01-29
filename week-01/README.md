# Week 1 - Introduction & Prerequisites (GCP, Docker, Terraform)

### Table of contents

1. [Introduction to GCP](#part-1)
2. [Introduction to Docker](#part-2)
3. [SQL Refresher](#part-3)
4. [Set up Google Cloud environment](#part-4)
5. [Infrastruture as Code with Terraform](#part-5)

    [Additional resources](#resource)

## Part 1: Introduction to GCP <a id='part-1'></a>

+ Google Cloud Platform is a cloud computing services offered by Google.
+ Includes a wide range of hosted services for computing, storage & application development that run on Google hardwares.
((Image of Google Services))

## Part 2: Introduction to Docker <a id='part-2'></a>

### Definition

+ Docker is a set of PaaS products that utilize OS-level virtualization to deliver software packages as a **container**.
+ Containers are isolated from one another and bundle their own software, libraries and configuration files; they can communicate with each other through well-defined channels.
+ Why Docker:
  + Reproductility: Containers can be built again and again with same environment configurations.
  + Local experiment: Containers are isolated and no effect each other or on local machines.
  + Integration test: For CI/CD.
  + Deploy and run on the Cloud (AWS, Azure, GCP)
  + Spark setup.
  + Serverless.

### 2.1 Setup Docker and basic commands

+ Install officical Docker on PC.
+ Test docker with hello-world project:  
```docker run hello-world```

+ Run Docker in interactive mode:  
```docker run -it ubuntu bash```

+ Run Docker with Python environment:  
```docker run -it python:3.9```

+ Run Docker with entry point:  
```docker run -it --entrypoint=bash python:3.9```

+ Create 'Dockerfile' to declare configuration for Docker:  

    ```docker
    FROM python:3.9

    RUN pip install pandas

    ENTRYPOINT [ "bash"] 
    ```  

+ Build Dockerfile in same directory:  
```docker build -t <name> .```

+ Run built Docker image:  
```docker run -it <name>```  
Ex: ```docker run -it test::pandas```

+ Create files to copy into Container: file ```pipeline.py```  

    ```python
    import pandas as pd

    print('Job finished successfully)
    ```

+ Declare COPY and working folder WORKDIR in Dockerfile:

    ```docker
    FROM python:3.9

    RUN pip install pandas

    WORKDIR /app
    COPY pipeline.py pipeline.py

    ENTRYPOINT [ "bash"] 
    ```

+ Pass arguments into Container:
  + In pipeline.py:

        ```python
        import sys
        ...
        print(sys.argv[0])
        ```

  + Modify dockerfile to run ```pipeline.py``` at start:

        ```docker
        ENTRYPOINT ["python", "pipeline.py"]
        ```

  + Command line:  

        ```bash
        docker run -it test::pandas <argument>
        ```

+ Check runing docker container:  

    ```bash
    docker ps
    ```

+ Kill a running container:

    ```bash
    docker kill <container id>
    ```

### 2.2. Run Postgres in Docker

+ Define Postgres environment in file ```docker-compose.yml```:

```yml
services:
  pgdatabase:
    image: postgres:13
    environment:
      - name=value
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data:rw
    ports:
      - "5432:5432"
    networks:
      - pg-network    
```

+ Command to run:

```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data:rw
    -p 5432:5432
postgres:13
```

+ Note:
  + -e or environment: define the environment variable for the container, which is posgres.
  + -v or volumnes: map the local folder on host machine to the folder on docker container, this is for save file records of postgres.
  + -p or ports: map the port from the host machine to the port in the docker container, this is for sending queries to postgres in container

+ Access the postgres database:

    ```bash
    pgcli -h localhost -p 5432 -u root -d ny_taxi
    ```

    Enter the password of the user.

+ Download New york Taxi Trip data: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

+ Read csv file with pandas in python:  

```python
from time import time
import pandas as pd
from sqlalchemy import create_engine

# syntax for connection url: "postgresql://<user>:<passowrd>@localhost:<port>/<database>"
engine = create_engine('posgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

# Read CSV by chunk to prevent memory overload
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

# create table in the database
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# Put data into table
while True:
    t_start = time()
    df = next(df_iter)
    
    # parse timestamp column
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create table in the database
    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()
    print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))
```

+ Run query with python and sqlalchemy (must install psycopg2-binary):

```python
query = """
SELECT * FROM yellow_tripdata LIMIT 10;
"""

pd.read_sql(query, con=engine)
```

### 2.3. Connect to postgres using pgAdmin

+ **pgAdmin** is a web-based UI tool to *interact* with Postgres database sessions, useful for performing database administration.

+ Download pgAdmin: <https://www.pgadmin.org/>

+ Create a network in Docker, which will be used to linked database in container to host machine:

    ```bash
    docker create network <networ_name>
    ```

+ Re-run the container of posgres to connect to the network:

    ```bash
    docker run -it \
        -e POSTGRES_USER="root" \
        -e POSTGRES_PASSWORD="root" \
        -e POSTGRES_DB="ny_taxi" \
        -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data:rw
        -p 5432:5432 \
        --network=pg-network \
    postgres:13
    ```

+ Run pgAdmin within Docker container:

    ```bash
    docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pg-admin \
    dpage/pgadmin4
    ```

+ Access pgAdmin on the host machine: <localhost:8080>  
 Then enter admin email & password

+ On pgAdmin, click Server -> Create Server... -> Fill information:
![pgAdmin create Server](./images/pgadmin_create.jpg)

### 2.4 Dockerize the Ingestion script

+ Create the ingestion script [ingest_data.py](./2_docker/ingest_data.py). Make sure the script can read the arguments from bash command.

+ Define the Dockerfile:

    ```Dockerfile
    FROM python:3.9

    RUN apt-get install wget
    RUN pip install pandas sqlalchemy psycopg2

    WORKDIR /app
    COPY ingest_data.py ingest_data.py

    ENTRYPOINT [ "python","ingest_data.py"]
    ```

+ Build the Dockerfile:

    ```bash
    docker build -t taxi_ingest:v001
    ```

+ Run the container with arguments:

    ```bash
    docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=localhost \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url=$(URL)
    ```

### 2.5. Running Postgres and pgAdmin with Docker-Compose

+ **Docker-Compose** is a tool for defining and running multiple services of a container using a single YAML file configuration.

+ Define the services (Postgres, pgAdmin) in [docker-compose.yml](./2_docker/docker-compose.yml):

    ```yml
    services:
      pgdatabase:
        image: postgres:13
          environment:
            - POSTGRES_USER=root
            - POSTGRES_PASSWORD=root
            - POSTGRES_DB=ny_taxi
          volumes:
            - ./ny_taxi_postgres_data:/var/lib/postgresql/data:rw
          ports:
            - "5432:5432"
          networks:
            - pg-network    

      pgadmin:
        image: dpage/pgadmin4
          environment:
            - PGADMIN_DEFAULT_EMAIL=admin@admin.com
            - PGADMIN_DEFAULT_PASSWORD=root
          ports:
            - "8080:80"
          networks:
            - pg-network    
    networks:
      pg-network:
        name: pg-network
    ```

+ Run the Docker-compose container:  

    ```bash
    docker-compose up
    ```

+ Shutdown the Docker-compose container:

    ```bash
    docker-compose down
    ```

## Part 3: SQL Refresher <a id='part-3'></a>

Refer to PostgreSQL document:
<https://www.postgresql.org/docs/current/index.html>

## Part 4: Set up Google Cloud environment <a id='part-4'></a>

+ Go to SSH directory  
```cd ~/.ssh/```

+ Generate SSH key:  
```ssh-keygen -t rsa -f gcp -C nda -b 2048```

+ View public key:  
```cat gcp.pub```

+ Copy the public key into metadata of GCP Compute engine

+ Create a VM instance on GCP

+ ssh into VM **using your private key**:  
++ Method 1:  
```ssh -i ~/.ssh/gcp nda@<external ip address>```
++ Method 2:  
Create config file:  

    ```bash
            Host de-zoomcamp  
                HostName 34.142.161.225  
                User nda  
                IdentityFile ~/.ssh/gcp  

        ssh into vm: ```ssh de-zoomcamp```
    ```

+ View machine info:  ```htop```

+ Download and install anaconda for VM:  
```wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh```  
```bash Anaconda3-2022-10-Linux-x86_64.sh```

## Part 5: Infrastructure as Code with Terraform <a id='part-5'></a>
