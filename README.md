# Data Engineering Project

This repository contains the implementation of a data engineering project, which includes ETL processes, SQL analysis, DBT models, and automation strategies.

## Project Overview

- The project uses Jupyter notebooks for ETL processes, SQL for data analysis, DBT for data modeling, and Databricks for automation.
- The related dbt repository can be found [here](https://github.com/huynguyen0257/demo-dbt).

## 1. Local Environment
### Setup env & docker compose

- Create a `.env` file from `.env.example` with the necessary environment variables.
- Use the provided `docker-compose` setup to run Jupyter. Open [Jupyter Notebook](http://127.0.0.1:8888)

### ETL Process

The ETL process is implemented in the Jupyter notebook `./notebooks/cus-trx-etl.ipynb`.

- You can run with Jupiter Notebook on http://127.0.0.1:8888.
- The result will create the `customer_transactions` table in Redshift and save Parquet files `customer_transactions.parquet` in the S3 bucket specified by `S3_BUCKET`.

### SQL Analysis

The SQL queries are implemented in `./sql/1.sql` and `./sql/2.sql`.

- Test the queries by copying and running them in the Redshift Query Data tool.

### DBT Models

The DBT models are implemented in the [dbt repository](https://github.com/huynguyen0257/demo-dbt). 
- Integrate with GitHub on DBT Labs and run `dbt run`.
- The result will create the `customer_daily_transactions` table in Redshift.

## 2. Infrastructure Setup

### Databricks:
- Since I have not used Databricks extensively, the exact setup steps will require experimentation and testing in a staging environment.
- Databricks will serve as the core tool for processing and automating the ETL flow from S3 to Redshift.

### AWS S3:
1. **Create a new S3 bucket** to store the transaction data files.
2. **Create a new IAM user** with the following permissions:
   - **S3 Access Policy**: Allows the IAM user to create and retrieve objects in the S3 bucket.
   - Generate the **access key** and **secret key** for this user, which will be used in the ETL process.
3. Use these credentials in your environment variables to allow your code to access and interact with the S3 bucket.

### DBT:
1. **Create a new DBT project**:
   - Set up the connection to Redshift, ensuring the credentials and configuration are correct.
   - Integrate the project with **GitHub** for version control, allowing smooth collaboration and management.
2. **DBT Model Execution**:
   - Schedule the DBT models to run daily via **DBT Job Scheduler**. This will keep the `customer_daily_transactions` table updated regularly.

### AWS Redshift:
1. For cost optimization, use **Redshift Serverless** to reduce infrastructure costs while maintaining performance.
2. Ensure **inbound rules** are configured in the security group to allow local machines and DBT to connect to Redshift securely.

## 3. ETL Setup

### ETL Processes:
- **Databricks**: Ideal for handling the ETL of new files from S3 to Redshift.
- **DBT**: Used for managing transformations and the creation of models in Redshift.

### Usage:
1. **Databricks**: 
    - **Job Scheduler** - Use cron job
    - **File Arrival Trigger** and **Auto Loader**
        - Utilize **File Arrival Trigger** and **Auto Loader** in Databricks to detect and ingest new files that arrive in the S3 bucket.
        - Ensure new files are created for chunks of new transactions with a postfix concatenated to the date-time format, e.g., `network_3_transactions_2024_09_20`.
        - This ensures that new data is moved into Redshift in real-time or at regular intervals.
2. **DBT Labs**
    - **Job Scheduler**:
        - Setup regular intervals (e.g., daily).
    - **Event-Driven**
        - Event-driven execution of dbt Cloud jobs triggered by API

## Automation

### Event-Driven Automation (Preferred)
- Use Databricks **File Arrival Trigger** and **Auto Loader** to insert new transactions into Redshift.
- This event-driven approach ensures the ETL process is triggered as soon as new files arrive in the S3 bucket.
- Ensure new files are created for chunks of new transactions with a postfix concatenated to the date-time format, e.g., `network_3_transactions_2024_09_20`.
- After the ETL process completes in **Databricks** -> (trigger) **DBT** job via API to update the models in Redshift.

### Cron-Style Automation
- As an alternative, use **Databricks Scheduled Jobs** to run the ETL process at regular intervals (e.g., daily).
- This approach is useful when you want to ensure the ETL process runs even if files do not arrive consistently.
- You can configure the **Scheduled Job** within Databricks to run the ETL notebook at your desired time intervals, ensuring the pipeline processes the data from S3 to Redshift on a set schedule.