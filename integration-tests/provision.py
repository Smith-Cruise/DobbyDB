#!/usr/bin/env python3
import subprocess
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "integration-tests" / "docker-compose.yml"
HOST_ENDPOINT = "http://127.0.0.1:5050"
SPARK_ENDPOINT = "http://moto:5000"
REST_CATALOG_SPARK_URI = "http://iceberg-rest:8181"
REGION = "us-east-1"
ACCESS_KEY = "test"
SECRET_KEY = "test"
BUCKET = "lakelet-e2e"
TABLE = "orders"
PAIMON_DATABASE = "paimon_db"
ICEBERG_DATABASE = "iceberg_db"
DELTA_DATABASE = "delta_db"
HIVE_DATABASE = "hive_db"
PAIMON_LOCATION = f"s3://{BUCKET}/warehouse/{PAIMON_DATABASE}.db/{TABLE}"
DELTA_LOCATION = f"s3://{BUCKET}/warehouse/{DELTA_DATABASE}.db/{TABLE}"
HIVE_LOCATION = f"s3://{BUCKET}/warehouse/{HIVE_DATABASE}.db/{TABLE}"
PAIMON_JARS = ",".join(
    [
        "/opt/spark/extra-jars/paimon-spark-4.0_2.13-1.4.1.jar",
        "/opt/spark/extra-jars/paimon-s3-1.4.1.jar",
    ]
)
ICEBERG_JARS = ",".join(
    [
        "/opt/spark/extra-jars/iceberg-spark-runtime-4.0_2.13-1.11.0.jar",
        "/opt/spark/extra-jars/iceberg-aws-bundle-1.11.0.jar",
    ]
)
DELTA_JARS = ",".join(
    [
        "/opt/spark/extra-jars/delta-spark_2.13-4.0.0.jar",
        "/opt/spark/extra-jars/delta-storage-4.0.0.jar",
        "/opt/spark/extra-jars/antlr4-runtime-4.13.1.jar",
        "/opt/spark/extra-jars/hadoop-aws-3.4.1.jar",
        "/opt/spark/extra-jars/bundle-2.24.6.jar",
    ]
)


def client(service: str):
    return boto3.client(
        service,
        endpoint_url=HOST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def wait_for_moto() -> None:
    s3 = client("s3")
    deadline = time.monotonic() + 60
    while True:
        try:
            s3.list_buckets()
            return
        except EndpointConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def ensure_bucket() -> None:
    s3 = client("s3")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def ensure_database(database: str) -> None:
    glue = client("glue")
    try:
        glue.create_database(DatabaseInput={"Name": database})
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "AlreadyExistsException":
            raise


def create_paimon_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            PAIMON_JARS,
            "--conf",
            "spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            "--conf",
            "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog",
            "--conf",
            f"spark.sql.catalog.paimon.warehouse=s3://{BUCKET}/warehouse",
            "--conf",
            f"spark.sql.catalog.paimon.s3.endpoint={SPARK_ENDPOINT}",
            "--conf",
            f"spark.sql.catalog.paimon.s3.access-key={ACCESS_KEY}",
            "--conf",
            f"spark.sql.catalog.paimon.s3.secret-key={SECRET_KEY}",
            "--conf",
            "spark.sql.catalog.paimon.s3.path.style.access=true",
            "--conf",
            f"spark.sql.catalog.paimon.s3.region={REGION}",
            "-f",
            "/integration-tests/create-paimon-table.sql",
        ],
        check=True,
    )


def create_iceberg_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            ICEBERG_JARS,
            "--conf",
            "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
            "--conf",
            "spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
            "--conf",
            "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf",
            f"spark.sql.catalog.iceberg.warehouse=s3://{BUCKET}/warehouse",
            "--conf",
            f"spark.sql.catalog.iceberg.glue.endpoint={SPARK_ENDPOINT}",
            "--conf",
            f"spark.sql.catalog.iceberg.client.region={REGION}",
            "--conf",
            f"spark.sql.catalog.iceberg.s3.endpoint={SPARK_ENDPOINT}",
            "--conf",
            "spark.sql.catalog.iceberg.s3.path-style-access=true",
            "--conf",
            f"spark.sql.catalog.iceberg.s3.access-key-id={ACCESS_KEY}",
            "--conf",
            f"spark.sql.catalog.iceberg.s3.secret-access-key={SECRET_KEY}",
            "-f",
            "/integration-tests/create-iceberg-table.sql",
        ],
        check=True,
    )


def create_iceberg_rest_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            ICEBERG_JARS,
            "--conf",
            "spark.sql.catalog.iceberg_rest=org.apache.iceberg.spark.SparkCatalog",
            "--conf",
            "spark.sql.catalog.iceberg_rest.type=rest",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.uri={REST_CATALOG_SPARK_URI}",
            "--conf",
            "spark.sql.catalog.iceberg_rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.warehouse=s3://{BUCKET}/warehouse",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.client.region={REGION}",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.s3.endpoint={SPARK_ENDPOINT}",
            "--conf",
            "spark.sql.catalog.iceberg_rest.s3.path-style-access=true",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.s3.access-key-id={ACCESS_KEY}",
            "--conf",
            f"spark.sql.catalog.iceberg_rest.s3.secret-access-key={SECRET_KEY}",
            "-f",
            "/integration-tests/create-iceberg-rest-table.sql",
        ],
        check=True,
    )


def create_delta_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            DELTA_JARS,
            "--conf",
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "--conf",
            "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint={SPARK_ENDPOINT}",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint.region={REGION}",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={ACCESS_KEY}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={SECRET_KEY}",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "--conf",
            "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "-f",
            "/integration-tests/create-delta-table.sql",
        ],
        check=True,
    )


def register_delta_table() -> None:
    glue = client("glue")
    try:
        glue.delete_table(DatabaseName=DELTA_DATABASE, Name=TABLE)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise

    glue.create_table(
        DatabaseName=DELTA_DATABASE,
        TableInput={
            "Name": TABLE,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "spark.sql.sources.provider": "delta",
                "table_type": "DELTA",
            },
            "StorageDescriptor": {
                "Columns": [],
                "Location": DELTA_LOCATION,
                "InputFormat": "org.apache.hadoop.mapred.FileInputFormat",
                "OutputFormat": "org.apache.hadoop.mapred.FileOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {},
                },
            },
        },
    )


def create_hive_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            DELTA_JARS,
            "--conf",
            "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint={SPARK_ENDPOINT}",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint.region={REGION}",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={ACCESS_KEY}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={SECRET_KEY}",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            "--conf",
            "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "-f",
            "/integration-tests/create-hive-table.sql",
        ],
        check=True,
    )


def register_hive_table() -> None:
    glue = client("glue")
    try:
        glue.delete_table(DatabaseName=HIVE_DATABASE, Name=TABLE)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise

    columns = [
        {"Name": "id", "Type": "int"},
        {"Name": "name", "Type": "string"},
        {"Name": "amount", "Type": "decimal(10,2)"},
    ]
    serde_info = {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "Parameters": {},
    }
    glue.create_table(
        DatabaseName=HIVE_DATABASE,
        TableInput={
            "Name": TABLE,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE"},
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
            "StorageDescriptor": {
                "Columns": columns,
                "Location": HIVE_LOCATION,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": serde_info,
            },
        },
    )
    glue.batch_create_partition(
        DatabaseName=HIVE_DATABASE,
        TableName=TABLE,
        PartitionInputList=[
            {
                "Values": [dt],
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"{HIVE_LOCATION}/dt={dt}",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": serde_info,
                },
            }
            for dt in ("2026-06-24", "2026-06-25")
        ],
    )


def register_paimon_table() -> None:
    glue = client("glue")
    try:
        glue.delete_table(DatabaseName=PAIMON_DATABASE, Name=TABLE)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise

    glue.create_table(
        DatabaseName=PAIMON_DATABASE,
        TableInput={
            "Name": TABLE,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "table_type": "PAIMON",
            },
            "StorageDescriptor": {
                "Columns": [],
                "Location": PAIMON_LOCATION,
                "InputFormat": "org.apache.paimon.hive.mapred.PaimonInputFormat",
                "OutputFormat": "org.apache.paimon.hive.mapred.PaimonOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.paimon.hive.PaimonSerDe",
                    "Parameters": {},
                },
            },
        },
    )


def main() -> None:
    wait_for_moto()
    ensure_bucket()
    ensure_database(PAIMON_DATABASE)
    ensure_database(ICEBERG_DATABASE)
    ensure_database(DELTA_DATABASE)
    ensure_database(HIVE_DATABASE)
    create_paimon_data()
    register_paimon_table()
    create_iceberg_data()
    create_iceberg_rest_data()
    create_delta_data()
    register_delta_table()
    create_hive_data()
    register_hive_table()


if __name__ == "__main__":
    main()
