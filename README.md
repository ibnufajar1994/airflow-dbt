# airflow-dbt
ETL Pipeline Orchestration Using Airflow &amp; DBT

# Pac-Flights Data Pipeline


## Table of Contents

1. [Overview](#overview)  
2. [Techstack](#techstack)  
3. [Pipeline Design](#pipeline-design)     
4. [Project Setup](#project-setup)  
5. [Documentation](#documentation)  



## Overview
This project is designed to develop and manage a robust data pipeline leveraging Apache Airflow for orchestration and DBT (Data Build Tool) for data transformation, utilizing Jinja templating for dynamic SQL generation. The pipeline begins by extracting data from a PostgreSQL database, which is then temporarily stored in object storage. Subsequently, the data is uploaded to the target data warehouse for further analysis or reporting.

The architecture of the pipeline supports incremental data loading, ensuring efficient updates by transferring only the new or modified records. It also incorporates XComs for inter-task communication, enabling tasks within Airflow to share data seamlessly. To enhance operational monitoring and responsiveness, the pipeline is equipped with Slack integration for real-time alerting.

Additional features include dynamic task generation, allowing the DAG to adjust to varying data or configurations at runtime, and modular configuration through a command-line interface (CLI) for greater flexibility and scalability. The entire system is orchestrated by Apache Airflow running with the CeleryExecutor, which enables distributed task execution across multiple workers, improving performance and reliability for production-grade workflows.

--- 

## Techstack

- **Data Base**: PostgreSQL 
- **Object Storage**: MinIO (`extracted-data` bucket)
- **Data Transformations Tools**: DBT (`warehouse` schema)
- **Orchestrator**: Apache Airflow
- **Notifier**: Slack Webhook 
- **Containerization**: Docker

---

## Pipeline Design

![ELT DAG](sample/PipelineDesign.png)

| Step | Process     | Tool              | Description                                               |
| ---- | ----------- | ----------------- | --------------------------------------------------------- |
| 1    | Extract     | Python + Airflow  | Extract data from source DB and dump it to MinIO          |
| 2    | Load        | Airflow + Pangres | Load the csv data to datawarehouse using upsert method    |
| 3    | Trigger DAG | Airflow           | Triggers pipeline using TriggerDagRun                     |
| 4    | Transform   | DBT + Airflow     | Transform data to final schema                            |

--- 

## Project Setup

Create a `.env` file in the root project with the following keys:

```env
# Airflow config
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=write-your-fernet-key
AIRFLOW_WEBSERVER_SECRET_KEY=write-your-webserver-secret-key
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_BROKER_URL=redis://:@redis:6379/0

# Warehouse (staging and dimensional)
WAREHOUSE_DB_USER=...
WAREHOUSE_DB_PASSWORD=...
WAREHOUSE_DB_NAME=warehouse_pacflight

# Source DB (bookings)
PACFLIGHT_DB_USER=...
PACFLIGHT_DB_PASSWORD=...
PACFLIGHT_DB_NAME=pacflight

# MinIO
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...

# Airflow Metadata DB
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
```

### Generate Fernet Key for Airflow

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then copy it into your `.env`:

```env
AIRFLOW_FERNET_KEY=your_generated_fernet_key
```


### Slack Notification Setup

To receive task failure notifications in Slack:

1. Create a Slack webhook via Slack App → Incoming Webhooks
2. Add the webhook URL into your `password` as `HTTP` connections
3. Ensure your DAG uses Slack alert logic in its failure callbacks


---



## How to Run the Pipeline

### 1. Clone the Repository

```bash
git clone://https://github.com/ibnufajar1994/airflow-dbt.git
```


### 2. Start All Services using docker compose

Make sure you already setup the .env and credentials in connections (password, secret key, etc), using WSL you can just start this script

```bash
docker compose up -d
```
---

### 3. Load all variable & connections via airflow cli

```bash
docker exec -it airflow-webserver bash
```

```bash
cd include 
```

```bash
airflow connections import connections.yaml
airflow variables import variables.json

```

### Access UI

| Service       | URL                                            |
| ------------- | ---------------------------------------------- |
| Airflow UI    | [http://localhost:8080](http://localhost:8080) |
| MinIO Browser | [http://localhost:9001](http://localhost:9001) |

---


## Documentation

### 1. DAG 

![DAG Staging](sample/1.dag-1.png)
![DAG Warehouse](sample/2.dag-2.png)

### 2. Task Groups

| Extract                                  |
| ---------------------------------------- |
| ![Extract Group](sample/3.taskgroup-extract.png) |

| Load                                  |
| ---------------------------------------- |
| ![Load Group](sample/4.taskgroup-load.png) | 

| DBT Transform                                  |
| ---------------------------------------- |
| ![Transform Group](sample/5.taskgroup-transform.png) |


### 3. Slack Notifications

![Slack Notifications](sample/6.slack-notification.png)

### 4. Sample Query

![Sample Query](sample/7.sample-query.png)
