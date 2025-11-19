import pytest
import os
from pathlib import Path
from dags import citibike_to_gcs_2025 as dag_module

# ------------------------
# Fixture pou folder tès
# ------------------------
@pytest.fixture
def setup_test_folder(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    return folder

# ------------------------
# Fixture pou CSV senp
# ------------------------
@pytest.fixture
def sample_csv(setup_test_folder):
    csv_path = setup_test_folder / "sample.csv"
    csv_path.write_text("started_at,ended_at,start_station_id,end_station_id\n2025-01-01,2025-01-01,1,2")
    return csv_path

# ------------------------
# Test format_to_parquet san CSV
# ------------------------
def test_format_to_parquet_no_csv(setup_test_folder, caplog, monkeypatch):
    # Patch DATA_FOLDER nan modil DAG la
    monkeypatch.setattr(dag_module, "DATA_FOLDER", str(setup_test_folder))
    dag_module.format_to_parquet()
    assert "No CSV files found" in caplog.text

# ------------------------
# Test format_to_parquet ak CSV
# ------------------------
def test_format_to_parquet_with_csv(setup_test_folder, sample_csv, monkeypatch):
    monkeypatch.setattr(dag_module, "DATA_FOLDER", str(setup_test_folder))
    dag_module.format_to_parquet()
    parquet_file = setup_test_folder / "sample.parquet"
    assert parquet_file.exists(), "Parquet file should be created"

# ------------------------
# Test upload_to_gcs san parquet
# ------------------------
def test_upload_to_gcs_no_files(setup_test_folder, caplog, monkeypatch):
    monkeypatch.setattr(dag_module, "DATA_FOLDER", str(setup_test_folder))
    dag_module.upload_to_gcs()
    assert "No Parquet files found" in caplog.text

# ------------------------
# Test upload_to_gcs ak parquet (mock GCSHook)
# ------------------------
def test_upload_to_gcs_with_file(setup_test_folder, monkeypatch):
    # Kreye fake parquet
    parquet_path = setup_test_folder / "sample.parquet"
    parquet_path.write_text("FAKE PARQUET DATA")
    
    # Patch DATA_FOLDER
    monkeypatch.setattr(dag_module, "DATA_FOLDER", str(setup_test_folder))
    
    # Mock GCSHook
    class FakeBlob:
        def upload_from_filename(self, filename):
            self.uploaded = filename

    class FakeBucket:
        def blob(self, object_name):
            return FakeBlob()
    
    class FakeGCSHook:
        def __init__(self, gcp_conn_id):
            pass
        def get_bucket(self, bucket_name):
            return FakeBucket()

    monkeypatch.setattr(dag_module, "GCSHook", FakeGCSHook)
    
    # Call upload
    dag_module.upload_to_gcs()
