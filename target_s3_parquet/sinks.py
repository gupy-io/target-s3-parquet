"""S3Parquet target sink class, which handles writing streams."""


import awswrangler as wr
from pandas import DataFrame, to_datetime
from singer_sdk.sinks import BatchSink

from target_s3_parquet.data_type_generator import generate_column_schema
from datetime import datetime

STARTED_AT = datetime.now()


class S3ParquetSink(BatchSink):
    """S3Parquet target sink class."""

    max_size = 10000  # Max records to write in one batch

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        df = DataFrame(context["records"])

        df["_sdc_started_at"] = to_datetime(STARTED_AT)

        dtype = generate_column_schema(self.schema["properties"])

        full_path = f"{self.config.get('s3_path')}/{self.config.get('athena_database')}/{self.stream_name}"

        wr.s3.to_parquet(
            df=df,
            index=False,
            compression="gzip",
            dataset=True,
            path=full_path,
            database=self.config.get("athena_database"),
            table=self.stream_name,
            mode="append",
            partition_cols=["_sdc_started_at"],
            schema_evolution=True,
            dtype=dtype,
        )

        self.logger.info(f"Uploaded {len(context['records'])}")

        context["records"] = []
