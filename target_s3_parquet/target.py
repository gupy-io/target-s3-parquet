"""S3Parquet target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_s3_parquet.sinks import S3ParquetSink


class TargetS3Parquet(Target):
    """Sample target for S3Parquet."""

    name = "target-s3-parquet"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "s3_path",
            th.StringType,
            description="The s3 path to the target output file",
            required=True,
        ),
        th.Property("aws_access_key_id", th.StringType, required=False),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            required=False,
        ),
        th.Property("athena_database", th.StringType, required=True),
        th.Property("add_record_metadata", th.BooleanType, default=None),
        th.Property("stringify_schema", th.BooleanType, default=None),
    ).to_dict()
    default_sink_class = S3ParquetSink
