
# -----------------------------
# Base image (Astro Runtime)
# -----------------------------
FROM astrocrpublic.azurecr.io/runtime:3.1-3

# -----------------------------
# Switch to root to install system packages
# -----------------------------
USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        vim \
        curl \
        unzip \
        lsb-release \
        apt-transport-https \
        git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Create directories for credentials and DBT configs
# -----------------------------
RUN mkdir -p /opt/airflow/.google/credentials && \
    mkdir -p /opt/airflow/.dbt && \
    mkdir -p /opt/airflow/dbt && \
    mkdir -p /opt/airflow/tests

# -----------------------------
# Copy GCP credentials into the container
# -----------------------------
COPY .google/credentials/google_credentials.json /opt/airflow/.google/credentials/google_credentials.json

# -----------------------------
# Copy DBT project and profiles
# -----------------------------
COPY .dbt/citibike_dbt_gcp/ /opt/airflow/dbt/
COPY .dbt/profiles.yml /opt/airflow/.dbt/profiles.yml

# -----------------------------
# Fix DBT permissions
# -----------------------------
RUN chown -R astro:astro /opt/airflow/dbt \
 && chmod -R 755 /opt/airflow/dbt

# -----------------------------
# Switch back to the Airflow (Astro) user
# -----------------------------
USER astro

# -----------------------------
# Install Python packages (Airflow, GCP, DBT)
# -----------------------------
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-bigquery \
    google-cloud-storage \
    pandas \
    pyarrow \
    apache-airflow-providers-google \
    apache-airflow-providers-dbt-cloud \
    astronomer-cosmos \
    astronomer-cosmos[google] \
    pytest

# -----------------------------
# Set environment variables for GCP, DBT, and Python paths
# -----------------------------
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/.google/credentials/google_credentials.json"
ENV DBT_PROFILES_DIR="/opt/airflow/.dbt"
ENV DBT_PROJECT_DIR="/opt/airflow/dbt"
ENV DBT_TARGET="dev"
ENV GCP_PROJECT_ID="de-citibike-data-pipeline"
ENV BIGQUERY_DATASET="de_citibike_tripdata_1"
ENV BUCKET="dtc_data_lake_de-citibike-data-pipeline"
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:/opt/airflow/tests"

# -----------------------------
# Set working directory
# -----------------------------
WORKDIR /opt/airflow

# -----------------------------
# Copy DAGs into the image
# -----------------------------
COPY dags/ /opt/airflow/dags/

