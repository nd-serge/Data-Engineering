from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
import pyarrow as pa
import pyarrow.parquet as pq


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
  

    gcs_bucket_name = 'data-engineering-course-412921-green-taxi'
    GCP_Project_Name = 'data-engineering-course-412921'
    root_path = gcs_bucket_name 

    table = pa.Table.from_pandas(df)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/src/gcp-service.json'
    fs_gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(table, 
        root_path=root_path,
        filesystem=fs_gcs, 
        partition_cols=["lpep_pickup_date"])
