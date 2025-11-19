import pytest
from airflow.models import DagBag
from unittest.mock import patch
from airflow.utils.task_group import TaskGroup


# Fake constructor that returns a real TaskGroup
def fake_dbt_constructor(*args, **kwargs):
    return TaskGroup(group_id="dbt_transformations")


@patch("cosmos.airflow.task_group.DbtTaskGroup", side_effect=fake_dbt_constructor)
def test_dbt_dag_loaded(mock_dbt):
    dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)

    # Verify DAG is loaded
    assert "citibike_dbt_workflow" in dag_bag.dags

    dag = dag_bag.get_dag("citibike_dbt_workflow")

    # Verify TaskGroup exists
    assert "dbt_transformations" in dag.task_group.children

    # Verify tasks exist
    assert dag.get_task("start")
    assert dag.get_task("end")
