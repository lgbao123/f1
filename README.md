
# F1 Racing Dashboard Project

## 1. Introduction 

This project aims to build a dashboard for F1 racing data visualization, providing insights into various aspects of Formula 1 races. The data is ingested, transformed, and stored using Azure Databricks, Azure Data Lake Storage, and Azure SQL Database. Power BI is used to create interactive reports and visualizations, while Azure Data Factory is employed for scheduling and monitoring data pipelines.
### Technologies used

- Databricks with PySpark
- Azure Data Lake
- Azure SQL Database
- Power BI
- Azure Data Factory

## 2. Implementation overview 
- <b>Data Ingestion</b>: Data is fetched from the [The Ergast API](https://ergast.com/mrd/) ( an experimental web service which provides a historical record of motor racing data for non-commercial purposes ) using <b>Databricks with pyspark</b>.

- <b>Data Storage</b>: Ingested data is stored in <b>Azure Data Lake</b> for further processing.
- <b>Data Transformation</b>: Data is transformed as per requirements using <b>Databricks</b>.
- <b>Data Warehousing</b>: Transformed data is stored in <b>Azure SQL Database</b> for efficient querying.
- <b>Dashboard Creation</b>: <b>Power BI</b> is used to create interactive and insightful dashboards.
- <b>Pipeline Orchestration</b>: <b>Azure Data Factory</b> is used for scheduling weekly and monitoring the data pipeline.

<img src = ./img/f1.png >

## 3. Visualize 

The dashboard provides insights into various aspects of F1 racing, including race results, driver performance, driver standings, and more. 

<img src = ./img/f1_dash.png>
