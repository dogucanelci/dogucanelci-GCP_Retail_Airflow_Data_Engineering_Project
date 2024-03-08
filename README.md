![alt text](images/image.png)

<h1 style="display: inline-block;"> Retail Data End to End Data Engineering Project on Google Cloud Platform Orchestrated by Airflow </h1>

## Dataset
The "Online Retail II" dataset contains transactions from a UK-based online retail company. The data covers the period from 01/12/2009 to 09/12/2011 and includes sales of unique all-occasion giftware. The customer base primarily consists of wholesalers.
Dataset Link: [Online Retail Dataset](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)


## 📝 Table of Contents
1. [Project Overview](#introduction) <br>
2. [Project and DAG Architecture](#project-architecture) <br>
  2.1. [Installation and Initialization](#install-initiate) <br>
  2.2. [Data Ingestion](#data-ingestion) <br>
  2.3. [Data Loading](#data-loading) <br>
  2.4. [Data Quality Check (Raw Data)](#soda_raw) <br>
  2.5. [Data Transformation and Modeling](#dbt_transform_modeling) <br>
  2.6. [Data Quality Check (Transformed Data)](#soda_transformed) <br>
  2.7. [Data Transformation (Dimensional to Multi-Dimensional)](#dbt_toplayer_tables) <br>
  2.8. [Data Quality Check (Top Layer Data)](#soda_toplayer) <br>
  2.9. [Data Reporting](#data-reporting) <br>
3. [Credits](#credits) <br>
4. [Contact](#contact) <br>

<a name="introduction"></a>
## 🔬 Project Overview 

This project is an end-to-end Data Engineering project orchestrated by Airflow, using multiple technologies, and applied on the Google Cloud platform. Online Retail Dataset is used for this project. <br>
In first step, Astro CLI and its dependicies are installed. Astro CLI is a open source commandline interface tool which is used for install, initialize and use airflow in local so easily. It also build file and folder template which is necessary for airflow usage. It creates a container to setup airflow, deploy and run defined dags in the container by using Docker. <br>
In next step,retail dataset is pushed to Google Bucket Storage from defined container environment(local env). Then it is loaded into Google BigQuery Data Warehouse. <br>
In next step, Data Quality check is applied by open source tool SODA. It has useful logging and alert functions to detect any inconsistencies during ETL processing of data with so basic yaml file definitions. After that, Raw Data is investigated and Data Modeling is applied by DBT, which is easy to implement, capable to high-lineage an open source tool used for transformation process of ETL. In this part, fact and dim tables are created into BigQuery Data Warehouse. <br>
After data quality check by SODA, top layer, multi dimensional tables are created by transformation of dbt and last part, Retail Analytic Report is created in Google Looker Studio by using top layer transformed data.
Each step which are described above represents a task in Retail DAG. You can see DAG structure and all tasks in detail in below.<br>
<a name="project-architecture"></a>
## 📝 Project Architecture

You can find the detailed information on the diagram below:

![alt text](images/project_structure.jpg)

---------------------------


Retail Dag Architecture:

![alt text](images/dag_structure.png)

---------------------------

<a name="install-initiate"></a>
### 📤 Installation and Initialization
- Astro CLI and its prerequisites are installed and Astro project is initialized.
- Astro project image is started and airflow server is published in localhost server.
- Retail dataset is copied into local 'include' file in retail astro project which is also copied into project container running airflow app.
- All initialization steps in Google Cloud Platform side is completed.(create new project, bucket, create service account json file with bigquery,storage admin role to access and connect GCP)
- Requirements are installed.

  
![alt text](images/image-1.png)

<a name="data-ingestion"></a>
### 📤 Data Ingestion
- retail.py file is created for Retail DAG implementation.
- Retail dataset is pushed to Google Bucket Storage from defined container environment(local env) by new task implementation in retail1 DAG as shown below:

![alt text](images/image-2.png)
  

<a name="data-loading"></a>
### ⚙️ Data Loading
- Raw data pushed into Bucket Storage is loaded into Google BigQuery Data Warehouse by implementing new 2 task in retail1 DAG.
- First task create an empty table with defined schema informations.
- Second task apply loading process.

![alt text](images/image-3.png)
![alt text](images/image-4.png)


<a name="soda_raw"></a>
### 📥 Data Quality Check(for Raw Data)
- Data Quality check is applied by open source tool SODA for raw data just loaded in BigQuery by creating a new task in retail1 DAG.

![alt text](images/image-9.png)

- This check consists basic steps like data type and column name.

![alt text](images/image-5.png)

<a name="dbt_transform_modeling"></a>
### 📊 Data Modeling and Transformation
- Data Modeling design is done and transformation scripts are implemented which is used by dbt to create new dimensional model in BigQuery.

![alt text](images/image-6.png)

![alt text](images/image-7.png)
  

<a name="soda_transformed"></a>
### 📊 Data Quality Check(for fact and dim tables)
- Data Quality check is applied by open source tool SODA for dimensional model data just transformed in BigQuery by creating a new task in retail1 DAG.

![alt text](images/image-10.png)

- This check consists detailed steps like:
    -   All weekdays are in range 0-6
    -   All datetimes,customers,products are unique
    -   Price,total amounts must be greater than zero etc.

![alt text](images/image-8.png)


<a name="dbt_toplayer_tables"></a>
### 📊 Data Transformation
- Data transformation scripts are implemented to create multi-dimensional top layer tables to feed the report directly by dbt tool.

![alt text](images/image-11.png)

- Process is implemented in retail1 DAG as task.

![alt text](images/image-12.png)

<a name="soda_toplayer"></a>
### 📊 Data Quality Check(for top-layer tables)
- Data Quality check is applied by open source tool SODA for multi-dimensional model data by creating a new task in retail1 DAG.

![alt text](images/image-13.png)

- This check consists detailed steps like:
    -   All customers have a country
    -   All products have a stock code etc.

<a name="data-reporting"></a>
### 📊 Data Reporting
- BigQuery is connected with Looker Studio BI , and used the Views of the DB to create interactive and insightful data visualizations.

![alt text](images/image-14.png)


### 🛠️ Technologies Used

- **Data Source**: Google Cloud Bucket Storage
- **Orchestration**: Airflow, Astro CLI, Docker
- **ETL Process**: Python
- **Transformation**: dbt tool
- **Data Quality Check**: SODA
- **Date Warehousing**: Bigquery, t-SQL
- **Storage**: Google Cloud Bucket Storage
- **Data Visualization**: Looker Studio

<a name="credits"></a>
## 📋 Credits

- This Project is inspired by the video of the [Data with Marc"](https://www.youtube.com/watch?v=DzxtCxi4YaA&t=179s)  

<a name="contact"></a>
## 📨 Contact Me

[LinkedIn](https://www.linkedin.com/in/elcidogucan/)
[Website](https://www.dogucanelci.com)
[Gmail](dogucanelci@gmail.com)
