## Table of Contents
- [General Info](#general-info)
- [Technologies](#technologies)

## General Info
In this project, I created a data pipeline for ingesting and processing daily stock market data from multiple stock exchanges. At a high level, my project consists of the following stages:

- Write CSV and JSON files to Azure Blob Storage
- Parse CSV and JSON files and write to Azure Blob Storage as Parquet files
- Load Parquet files in order to perform data cleaning and processing
- Used Azure Databricks for prototyping
- Creation and configuration of Azure Virtual Machine
- Apache Airflow DAG using SSHOperators for orchestrating data pipeline on Azure Virtual Machine

## Technologies
Project is created with: 
* Python (supporting libraries)
* PySpark
* Apache Airflow
* Azure Blob Storage
* Azure Databricks
* Azure Virtual Machine
* cloudpathlib