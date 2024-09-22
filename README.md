# Setup

### Run Jupyter with Docker Compose:

- Create a `.env` file from `.env.example` with the necessary environment variables.
- Use the provided `docker-compose` setup to run Jupyter. Open [Jupyter Notebook](http://127.0.0.1:8888)

## Question 1 & 2: ETL Process

The ETL process is implemented in the Jupyter notebook `./notebooks/cus-trx-etl.ipynb`.

- You can run with Jupiter Notebook on http://127.0.0.1:8888.
- The result will create the `customer_transactions` table in Redshift and save Parquet files `customer_transactions.parquet` in the S3 bucket specified by `S3_BUCKET`.

## Question 3: SQL Analysis

The SQL queries are implemented in `./sql/1.sql` and `./sql/2.sql`.

- Test the queries by copying and running them in the Redshift Query Data tool.

## Question 4: DBT Models

The DBT models are implemented in the `./dbt` folder.

- Integrate with GitHub on DBT Labs and run `dbt run`.
- The result will create the `customer_daily_transactions` table in Redshift.

## Question 5: Automation

### Event-Driven Automation (Preferred)
- Use Databricks **File Arrival Trigger** and **Auto Loader** to insert new transactions into Redshift.
- This event-driven approach ensures the ETL process is triggered as soon as new files arrive in the S3 bucket.
- Ensure new files are created for chunks of new transactions with a postfix concatenated to the date-time format, e.g., `network_3_transactions_2024_09_20`.

### Cron-Style Automation
- As an alternative, use **Databricks Scheduled Jobs** to run the ETL process at regular intervals (e.g., daily).
- This approach is useful when you want to ensure the ETL process runs even if files do not arrive consistently.
- You can configure the **Scheduled Job** within Databricks to run the ETL notebook at your desired time intervals, ensuring the pipeline processes the data from S3 to Redshift on a set schedule.



# Details of Implementation

## 1. Python and Jupyter Notebook for ETL Testing

We will first implement the ETL process using Python and Jupyter Notebook. This allows us to test the ETL flow from AWS S3 to Redshift in a controlled environment. After successfully completing the ETL process within Jupyter, we can move the code to Databricks for production use, leveraging its capabilities to run the Python script on a larger scale.

## 2. ETL Setup: Databricks and DBT

### ETL Process Locations:
- **Databricks**: Ideal for handling the ETL of new files from S3 to Redshift.
- **DBT**: Used for managing transformations and the creation of models in Redshift.

### Recommended Flow:
1. **File Arrival Trigger & Auto Loader (Databricks)**: 
   - Utilize **File Arrival Trigger** and **Auto Loader** in Databricks to detect and ingest new files that arrive in the S3 bucket.
   - Ensure new files are created for chunks of new transactions with a postfix concatenated to the date-time format, e.g., `network_3_transactions_2024_09_20`.
   - This ensures that new data is moved into Redshift in real-time or at regular intervals.
2. **DBT Job Scheduler (Event-Driven)**:
   - After the ETL process completes in Databricks, trigger a DBT job to update the models in Redshift.
   - Event-driven scheduling is preferred, ensuring that DBT models are only refreshed after new data is available.

## 3. Infrastructure Setup

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

## 4. Conclusion
By setting up Databricks for file ingestion and DBT for transformation, we can ensure an efficient, automated pipeline. AWS S3 and Redshift will serve as the storage and processing layers, while DBT will manage the final transformation logic and model updates.
