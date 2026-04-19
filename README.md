#DataForge

This project demonstrate end to end pipeline using:

- Kafka (Streaming)
- Python ETL
- Airflow (Orchestration)
- Snowflake (Data Warehouse)
- dbt (Transformation)
- AWS
- Terraform (IaC)
- Docker Containerization

# Overview

This project implements a fully automated end-to-end data pipeline that processes real-time data using a streaming architecture and transforms it into analytics-ready datasets.

- Data is generated and streamed using Kafka (Producer → Consumer)
- Consumer data is stored in AWS S3
- Data is loaded into Snowflake (Bronze layer)
- Data is transformed using dbt into Silver and Gold layers
- Airflow orchestrates ingestion and transformation workflows
- Terraform is used for infrastructure setup
- CI/CD pipeline ensures automated deployment and validation


# Architecture
Medallion Architecture (Bronze - Silver - Gold)