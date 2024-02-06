# Week 2 - Workflow Orchestration

### Table of contents:
1. [Basic definitions](#part-1)
2. [Basic Workflow orchestration with Mage](#part-2)
3. [ETL with GCP & Prefect](#part-3)
4. [From Google Cloud Storage to BigQuery](#part-4)
5. [Parameterization and Deployments](#part-5)
6. [Scheduling and Containerization](#part-6)  

    [Additional resources](#resource)

## Part 1: Basic Definitions <a id='part-1'></a> (from 2023 cohort)

### Data Lake vs. Data Warehouse

![Alt text](images/image-1.png)
![Alt text](images/image-2.png)

### ETL vs. ELT

+ ETL (Extract-Transform-Load) is mainly for small amount of data --> Using for Data warehouse
+ ELT (Extract-Load-Transform) is schema-on-read to quickly store large amount of data--> Using for Data Lake

### Gotcha of Data Lake

+ Converting into Data Swamp
+ No versioning
+ Incompatible schemas for same data without versioning
+ No metadata associated
+ Joins not possible

### Cloud provider for Data Lake

+ GCP: Cloud Storage.
+ AWS: S3.
+ Azure: Azure Blob.

## Part 2: Basic Workflow Orchestration with Mage <a id='part-2'></a>

### Workflow Orchestration

+ **Orchestration** is the process of  dependency management, facicilated through automation.

+ The **data orchestration** manages scheduling, triggering, monitoring, even resource allocation.

+ Why we need orchestration:
  + Every workflow requires sequential steps.
  + Poorly sequential transformations brew a storm far more bitter.
  + Steps = tasks.

+ What is a good solution for workflow orchestration?
  + Worflow management.
  + Automation.
  + Error handling.
  + Recovery.
  + Monitoring, alerting.
  + Resource optimization.
  + Observibility.
  + Debugging.
  + Compliance/Auditing.

+ A good orchestration prioritizes... ***The developer experience***
  + Flow state.
    + *I need to switch between 7 tools/services*.
  + Feedback Loops.
    + *I spent 5 hours locally testing this DAG*.
  + Cognitive Load.
    + *How much do you need to know to do your job.*

+ An **orchestrator** is like a ***a conductor***.

### What is Mage?

+ **Mage**: An open-source pipeline tool for orchestraing, transforming, and integrating data.

+ Main concepts of Mage:
![Alt text](images/image-3.png)!

+ Mage accelerates pipeline development:
  + Hybrid environment:
    + Use your GUI for interative development (or don't, like VSCode).
    + Use blocks as testable, resuable piece of code.
  + Improve Developer Experience (DevEx):
    + Code and test in parallel.
    + Reduce your dependencies, switch tool less, be efficient.

+ Mage offers Engineering best practices built-in:
  + In-line testing and debugging.
    + *Familar, notebook-style format.*
  + Fully-featured observability.
    + *Transformations in one-place:* dbt model, streaming and etc.
  + DRY principles:
    + *No more DAGs with duplicated functions and weird imports.*
    + *DEaaS (Data Engineering as a Service).*
  + --> Reduce time in **undifferentiated** work

### Introduction to Mage concepts

+ Important Concepts: **Projects, Pipelines, Blocks.**
    ![Alt text](images/image-4.png)

+ **Projects**
  + A project forms the basis for all the work you can do in Mage— you can think of it like a GitHub repo. 
  + It contains the code for all of your pipelines, blocks, and other assets.
  + A Mage instance has one or more projects

+ **Piplines**
  + A pipeline is a workflow that executes some data operation— maybe extracting, transforming, and loading data from an API. They’re also called DAGs on other platforms
  + In Mage, pipelines can contain Blocks (written in SQL, Python, or R) and charts. 
  + Each pipeline is represented by a YAML file in the “pipelines” folder of your project.

+ **Blocks**
  + A block is a file that can be executed independently or within a pipeline. 
  + Together, blocks form Directed Acyclic Graphs (DAGs), which we call pipelines. 
  + A block won’t start running in a pipeline until all its upstream dependencies are met.
  + Blocks are reusable, atomic pieces of code that perform certain actions. 
  + Changing one block will change it everywhere it’s used, but don’t worry, it’s easy to detach blocks to separate instances if necessary.
  + Blocks can be used to perform a variety of actions, from simple data transformations to complex machine learning models. 

    ![Alt text](images/image-5.png)

### Configure Mage

+ Clone Mage configuration from Github: https://github.com/mage-ai/mage-zoomcamp

+ From  Shell, pull the lates Mage image: 

    ```bash
    docker pull mageai/mage:latest
    ```

+ Build the image:
    
    ```bash
    cd mage-zoomcamp
    docker compose build
    ```

+ Create the Mage container:

    ```bash
    docker-compose up
    ```

+ Access Mage UI from url: http://localhost:6789/





## Part 3: ETL with GCP & Prefect <a id='part-3'></a>

+ Create new python file ```etl_web_to_gcs.py```.

+ Import required libraries

    ```python
    from pathlib import Path
    import pandas as pd
    from prefect import flow, task
    from prefect.tasks import task_input_hash
    from prefect_gcp.cloud_storage import GcsBucket
    ```

+ Define the main ETL **flow** with multiple **tasks** to run:
    ```python
    @flow()
    def etl_web_to_gcs() -> None:
        """The main ETL function"""
        color = 'yellow'
        year = 2021
        month = 1
        dataset_file = f'{color}_tripdata_{year}-{month:02}.csv.gz'
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}'
        
        df = fetch(dataset_url)
        df_clean = clean(df)
        
        path = write_local(df_clean, color, dataset_file)
        write_gcs(path)
        pass
    ```

+ Define the **task** function to fetch data from web URL:

    ```python
    @task(log_prints=True, retries=3)
    def fetch(url: str) -> pd.DataFrame:
        """Read taxi data from web into pandas Dataframe"""
        print(url)
        df = pd.read_csv(url)
        return df
    ```

+ Define the **task** function to clean data:

    ```python
    @task(log_prints=True)
    def clean(df = pd.DataFrame) -> pd.DataFrame:
        """Fix dtype issue"""
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        return df   
    ```

+ Define the **task** function to write cleaned data locally as parquet file format:

    ```python
    @task(log_prints=True)
    def write_local(df, color, dataset_file) -> Path:
        """Write dataframe out locally as parquet file"""
        path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
        df.to_parquet(path, compression='gzip')
        return path
    ```
  + As a parquet format, we should define the compression method (gzip, pyarrow, etc.)

+ Define the **task** function to upload the parquet file into GCS Bucket:
  + On GCP, create an account (if not have).
  + Create a new GCS Bucket with globally unique name.
  + Create a service account with necessary permissions so as to interact with GCP Cloud Storage. Permission here: **Storage Admin**.
  + Download the JSON key of the service account.
  + Register GCP blocks in Prefect:  
    ```prefect block register -m prefect_gcp```
  + On Prefect UI, create a GCP Credentials block with the required information (key).
  + Create a GCP Bucket block and link to the credential block.
  + Load the block in python code and define the **task** function:
    ```python
    @task(log_prints=True)
    def write_gcs(path: Path) -> None:
        """Upload local parquet file to GCS"""
        gcs_block = GcsBucket.load("gcs-bucket-block")
        gcs_block.upload_from_path(
            from_path=path,
            to_path=path
        )
    ```

  + Go to the GCS Bucket and check if the file is successfully uploaded.

## Part 4: From Google Cloud Storage to BigQuery <a id='part-4'></a>

+ Create new python file ```etl_gcs_to_bq.py```

+ Import the required libraries:
    ```python
    from pathlib import Path
    import pandas as pd
    from prefect import flow, task
    from prefect_gcp.cloud_storage import GcsBucket
    from prefect_gcp import GcpCredentials
    ```

+ Define the main **flow**:
    ```python
    @flow()
    def etl_gcs_to_bq(color, year, month) -> None:
        """The main ETL function to load data to BigQuery"""
        
        path = extract_from_gcs(color, year, month)
        
        df = transform_data(path) # do nothing but read dataframe only
        write_to_bq(df)
        print(f"BQ: written {len(df)} rows successfully into Bigquery")
        pass
    
    if __name__ == '__main__':
        etl_gcs_to_bq('yellow', 2021, 11)
    ```

+ Define the **task** function to extract data from GCS using Prefect blocks:
    ```python
    @task(log_prints=True, retries=3)
    def extract_from_gcs(color: str, year: str, month: str) -> Path:
        """Download tripdata from GCS"""
        dataset_file = f'{color}_tripdata_{year}-{month:02}.csv.gz'
        gcs_path = f"data/{color}/{dataset_file}.parquet"
        
        gcs_block = GcsBucket.load("gcs-bucket-block")
        gcs_block.get_directory(
            from_path=gcs_path, 
            local_path=f'../data/'
        )
        return Path(f'../data/{gcs_path}')
    ```

+ Define the **task** function to do simple data transformation:

    ```python
    @task(log_prints=True)
    def transform_data(path: Path) -> pd.DataFrame:
        """Brief data cleaning"""    
        df = pd.read_parquet(path)
        print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
        df['passenger_count'] = df['passenger_count'].fillna(0)
        print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
        return df
    ```

+ On GCP, go to Biquery service, create a new dataset and a table from parquet file in GCS Bucket (uploaded from Part 3). This dataset and table will be the destination of our flow.

+ Make sure to add the Bigquery access permission to your service account (created from Part 3).

+ Define the **task** function to write cleaned data into BigQuery table:
    ```python
    @task(log_prints=True)
    def write_to_bq(df: pd.DataFrame) -> None:
        """Write Dataframe into Bigquery"""
        gcp_credentials_block = GcpCredentials.load("nda-gcp-credentials")
        df.to_gbq(
            destination_table='ny_taxi_trips.yellow_trips_data',
            project_id='nda-de-zoomcamp',
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=50_000,
            if_exists='append'
        )
    ```

+ Execute the flow:  
    ```bash
    python etl_gcs_to_bq.py
    ```

+ Go to Bigquery and perform a query to check if data is written succesfully.
    ```SQL
    SELECT count(*)
    FROM `ny_taxi_trips.yellow_trips_data`
    ```


## Part 5: Parameterization and deployments  <a id='part-5'></a>


+ In this section, we will parameterize the ETL flow of web-to-gcs.

### Flow parameterization
+ Parameterizing allows you to configure and generalize the **flow** with parameters.

+ First, add the parameters to your main **flow** function:

    ```python
    @flow()
    def etl_web_to_gcs(color: str, year: int, month: int) -> None:
        """The main ETL function"""
        # ....
    ```

+ Add the parent flow to execute multiple **flows**:

    ```python
    @flow(name='parent_flow')
    def etl_parent_flow(color: str = 'yellow',
                        year: int = 2021,
                        months: list[int] = [1, 2]
    ) -> None:
        # ....
    ```
  + This flow will run multiple main **flows** to upload taxi data of multiple months in a year.

+ The ```__main__``` function:
    ```python
    if __name__ == '__main__':
        color = 'yellow'
        year = 2021
        months = [1, 2, 3] 
        etl_parent_flow(color, year, months)
    ```

+ Let cache the ```fetch``` function:
    ```python
    from datetime import timedelta
    from prefect.tasks import task_input_hash

    @task(log_prints=True,
          retries=3,
          cache_key_fn=task_input_hash,
          cache_expiration=timedelta(days=1))
    def fetch(url: str) -> pd.DataFrame:
        """Read taxi data from web into pandas Dataframe"""
        # ....
    ```

+ Leave the other **task** funtions as is. Execute the parameterized **flow**:
    ```bash
    python parameterization_flow.py
    ```

### Prefect Deployments

+ **Prefect Deployment** is a server-side concept that encapsulates the flow, allowing it to be *scheduled* and *triggered* via the API.

+ Document for Prefect Deployment: https://docs.prefect.io/latest/concepts/deployments/

+ Build the deployment:  
    ```bash
    prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
    ```
  + We should define which is the entry flow in the python code, in this case, the ```etl_parent_flow``` flow.

+ Prefect will create a YAML file that contains the metadata of the deployment.

+ We can adjust the metadata in the YAML file or on the Prefect UI.

+ Apply the **flow** to Prefect API:
    ```bash
    prefect deployment apply etl_parent_flow-deployment.yaml
    ```

+ To execute the **flow**, now we need an **agent**, which is a execution environment to run the flow, in our case, our local machine.

+ On Prefect UI, go to **Work Queues** to check available agents. There is always a ```default``` agent.

+ Let's start the ```default``` agent:
    ```bash
    prefect agent start --work-queue "default"
    ```

+ Go to Prefect UI, go to **Deployments** to check your deployed **flow**. As it was parameterized, we can adjust the parameters of the **flow**.

+ Click **Quick run** to trigger the **flow**.

### Prefect Notifications

+ **Prefect Notifications** allows to alert the failure or success of a flow run through multiple kinds of channel.

+ On Prefect UI, go to **Notifications** and click **Create Notification**.
  + Pick a **Run state** to trigger the notification.
  + Select and configure the notification channel you want to send the alert.
  + Click **Create** to generate the notification

    ![Alt text](images/image-3.png)

## Part 6: Scheduling and Containerization <a id='part-6'></a>

### Flow Scheduling

+ On Prefect UI, go to **Deployments**, select your deployed flow.
+ Under **Schedule**, click **Add** to add a new schedule.
+ Select your time schedule as you would like to have.
![Alt text](images/image-4.png)

+ We can add multiple schedules for one **flow**.

+ We add schedule to the **flow** when deploying using *cron*:
    ```bash
    prefect deployment build parameterized_flow.py:etl_parent_flow -n "ETL 2" --cron "0 0 * * *" -a
    ```

### Dockerize the flow in container

+ Create a ```docker-requirement.txt``` to list the required libraries for the image:
    ```txt
    pandas==1.5.2
    prefect==2.7.7
    prefect-sqlalchemy==0.2.2
    prefect-gcp[cloud_storage]==0.2.4
    protobuf==4.21.11
    pyarrow==10.0.1
    pandas-gbq==0.18.1
    psycopg2-binary==2.9.5
    sqlalchemy==1.4.46
    ```

+ Create and define a ```Dockerfile```:
    ```Dockerfile
    FROM prefecthq/prefect:2.7.7-python3.10

    COPY docker-requirements.txt .

    RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org

    COPY 02_gcp /opt/prefect/flows
    ```

+ Build the Docker image:
    ```bash
    docker image build -t ngducanh1611/prefect:flow
    ```

+ Push the image to Docker Hub:
    ```bash
    docker image push ngducanh1611/prefect:flow
    ```

+ On Prefect, create a **Docker container** block. Define the image that you built for the flow.
  + Set ```Auto Remove``` to ```On```.

+ We need to deploy the **flow** from the python code inside the docker image.

+ Create a python file ```docker_deploy.py```:
    ```python
    from prefect.deployments import Deployment
    from prefect.infrastructure.docker import DockerContainer
    from parameterization_flow import etl_parent_flow

    docker_container_block = DockerContainer.load("docker-block")

    docker_dep = Deployment.build_from_flow(
        flow=etl_parent_flow,
        name='docker-flow',
        infrastructure=docker_container_block
    )

    if __name__ == "__main__":
        docker_dep.apply()
    
    ```

+ Execute python code to deploy the flow:
    ```bash
    python docker_deploy.py
    ```

+ Set the Prefect UI endpoint url:
    ```bash
    prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
    ```
  + This will allow the Docker container to interact with the Prefect Orion server.
  + If we have a Prefect cloud, we should set the URL to your Orion on the cloud.

+ Start the ```default``` agent:
    ```bash
    prefect agent start --work-queue "default"
    ```

+ Execute the **flow**:
    ```bash
    prefect deployment run etl-prent-flow/docker-flow -p "months=[1,2]"
    ```
  + ```-p``` allows to override the parameters of the **flow**.

+ Check the **flow run**  on Prefect UI.

## Additional resources <a id='resource'></a>

+ Prefect official document: https://docs.prefect.io/latest/
+ Prefect Cloud: https://app.prefect.cloud/
+ Prefect Community: https://discourse.prefect.io
