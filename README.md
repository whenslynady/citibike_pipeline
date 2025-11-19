🚴 Citibike Data Engineering Pipeline — End-to-End Project
📌 Overview

This project implements an end-to-end data pipeline using cloud technologies, IaC, workflow orchestration, data warehousing, and BI dashboards.
It follows all requirements of the Capstone Project and demonstrates a complete modern data engineering ecosystem.

The goal is to:

Ingest Citibike trip data

Store raw files in Google Cloud Storage (data lake)

Load data into BigQuery (data warehouse)

Transform data using dbt

Build a dashboard with at least two visualizations

Ensure full reproducibility using Docker, Astro (Airflow), Terraform, and CI/CD

🧱 Architecture
           +-------------------------+
           |  Citibike Public Data   |
           +-----------+-------------+
                       |
                       v
        +--------------+----------------+
        |    Airflow (Astro Runtime)   |
        |         Batch Pipeline       |
        +--------------+---------------+
                       |
               RAW CSV → GCS (Data Lake)
                       |
                       v
                BigQuery (Warehouse)
                       |
                       v
               dbt Transformations
                       |
                       v
            Analytics Tables (dim + fact)
                       |
                       v
        BI Dashboard (Looker Studio/Tableau)

🚀 Technologies Used
Layer	Tools
Orchestration	Astro Runtime (Airflow 3.1) via astro dev init
Containers	Docker, Docker Compose
Infrastructure as Code	Terraform (GCP resources)
Cloud Provider	Google Cloud Platform
Data Lake	Google Cloud Storage (GCS)
Data Warehouse	BigQuery (partitioned + clustered tables)
Transformations	dbt-core + dbt-bigquery
Testing	Pytest, SQL/dbt tests
Automation	Makefile
CI/CD	GitHub Actions (Airflow tests + dbt tests + Terraform validate + Deploy to Astro)
Dashboard	Looker Studio / Tableau