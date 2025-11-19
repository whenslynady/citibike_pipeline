# ---------------------------------------------------------
# VARIABLES
# ---------------------------------------------------------

# ETL container
CONTAINER_ETL=de-citibike-data-pipeline-airflow-scheduler-1

# DBT container
CONTAINER_DBT=citibike-pipeline_ba8a28-scheduler-1

LOCAL_DAGS_DIR=/home/whensly/citibike_pipeline/dags
LOCAL_TESTS_DIR=/home/whensly/citibike_pipeline/tests
CONTAINER_DAGS_DIR=/opt/airflow/dags
CONTAINER_TESTS_DIR=/opt/airflow/tests
DBT_DIR=/opt/airflow/dbt
PROFILES_DIR=/opt/airflow/.dbt
TERRAFORM_DIR=/home/whensly/citibike_pipeline/terraform

# ---------------------------------------------------------
# Copy short ETL DAG + tests into the ETL container
# ---------------------------------------------------------
copy-tests:
	docker exec -it $(CONTAINER_ETL) mkdir -p $(CONTAINER_DAGS_DIR)
	docker exec -it $(CONTAINER_ETL) mkdir -p $(CONTAINER_TESTS_DIR)
	docker cp $(LOCAL_DAGS_DIR)/citibike_to_gcs_2025.py $(CONTAINER_ETL):$(CONTAINER_DAGS_DIR)/
	docker cp $(LOCAL_TESTS_DIR)/test_citibike_to_gcs.py $(CONTAINER_ETL):$(CONTAINER_TESTS_DIR)/

# ---------------------------------------------------------
# Copy DBT workflow + tests into the ETL container
# ---------------------------------------------------------
copy-dbt-tests:
	docker exec -it $(CONTAINER_ETL) mkdir -p $(CONTAINER_DAGS_DIR)
	docker exec -it $(CONTAINER_ETL) mkdir -p $(CONTAINER_TESTS_DIR)
	docker cp $(LOCAL_DAGS_DIR)/citibike_dbt_workflow.py $(CONTAINER_ETL):$(CONTAINER_DAGS_DIR)/
	docker cp $(LOCAL_TESTS_DIR)/test_citibike_dbt_workflow.py $(CONTAINER_ETL):$(CONTAINER_TESTS_DIR)/

# ---------------------------------------------------------
# Enter ETL or DBT container bash shell
# ---------------------------------------------------------
bash-etl:
	docker exec -it $(CONTAINER_ETL) bash

bash-dbt:
	docker exec -it $(CONTAINER_DBT) bash

# ---------------------------------------------------------
# Run pytests for the ETL short DAG
# ---------------------------------------------------------
test-etl:
	docker exec -it $(CONTAINER_ETL) bash -c "\
	export PYTHONPATH=\$$PYTHONPATH:/opt/airflow && \
	cd $(CONTAINER_TESTS_DIR) && \
	pytest test_citibike_to_gcs.py -v \
	"

# ---------------------------------------------------------
# Run pytests for the DBT workflow (without Cosmos)
# ---------------------------------------------------------
test-dbt:
	docker exec -it $(CONTAINER_ETL) bash -c "\
	export PYTHONPATH=\$$PYTHONPATH:/opt/airflow && \
	cd $(CONTAINER_TESTS_DIR) && \
	pytest test_citibike_dbt_workflow.py -v \
	"

# ---------------------------------------------------------
# Full test suite (ETL + DBT)
# ---------------------------------------------------------
test-all: test-etl test-dbt

# ---------------------------------------------------------
# DBT workflow inside the DBT container
# ---------------------------------------------------------
dbt-all:
	docker exec -it $(CONTAINER_DBT) bash -c "\
	cd $(DBT_DIR) && \
	dbt debug --profiles-dir $(PROFILES_DIR) --project-dir $(DBT_DIR) && \
	dbt deps --profiles-dir $(PROFILES_DIR) --project-dir $(DBT_DIR) && \
	dbt run --profiles-dir $(PROFILES_DIR) --project-dir $(DBT_DIR) && \
	dbt test --profiles-dir $(PROFILES_DIR) --project-dir $(DBT_DIR) \
	"

# ---------------------------------------------------------
# Step 3: Verify Terraform workflow
# ---------------------------------------------------------
verify-terraform:
	@echo "---------------------------------------------------------"
	@echo "Initializing Terraform..."
	cd $(TERRAFORM_DIR) && terraform init
	@echo "---------------------------------------------------------"
	@echo "Planning Terraform..."
	cd $(TERRAFORM_DIR) && terraform plan
	@echo "---------------------------------------------------------"
	@echo "Dry-run apply (without modifying resources)..."
	cd $(TERRAFORM_DIR) && terraform apply -refresh-only -auto-approve
	@echo "---------------------------------------------------------"
	@echo "Listing Terraform files:"
	@ls -l $(TERRAFORM_DIR)

# ---------------------------------------------------------
# Run EVERYTHING (ETL tests + DBT workflow + Terraform)
# ---------------------------------------------------------
run-all: copy-tests copy-dbt-tests test-all dbt-all verify-terraform
	@echo "---------------------------------------------------------"
	@echo "    FULL PIPELINE SUCCESSFULLY EXECUTED"
	@echo "---------------------------------------------------------"
