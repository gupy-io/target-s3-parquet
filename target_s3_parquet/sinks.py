"""S3Parquet target sink class, which handles writing streams."""


import awswrangler as wr
from pandas import DataFrame, to_datetime
from singer_sdk.sinks import BatchSink

from target_s3_parquet.data_type_generator import generate_column_schema
from target_s3_parquet import flattening
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

        #       flattened_schema = {}
        #       flattened_records = []

        #       if self.config.get("flattening_enabled"):
        #           self.logger.info(
        #               f"SCHEMA: {self.schema['properties']} STREAM_NAME: {self.stream_name}"
        #           )

        #           flattened_schema = flattening.flatten_schema(self.schema, max_level=10)

        #           for record in context["records"]:
        #               flatten_record = flattening.flatten_record(
        #                   record, flattened_schema, max_level=10
        #               )
        #               flattened_records.append(flatten_record)

        #       if flattened_records:
        #           df = DataFrame(flattened_records)
        #       else:
        #           df = DataFrame(context["records"])

        #       if flattened_schema:
        #           schema = flattened_schema
        #       else:
        #           schema = self.schema["properties"]

        df = DataFrame(context["records"])

        df["_sdc_started_at"] = to_datetime(STARTED_AT)

        self.logger.info(self.schema)

        dtype = generate_column_schema(
            self.schema["properties"], only_string=self.config.get("stringify_schema")
        )

        if self.config.get("stringify_schema"):
            df = df.astype(str)

        self.logger.debug(f"DType Definition: {dtype}")

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
