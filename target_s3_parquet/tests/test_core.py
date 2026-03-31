"""Tests standard target features using the built-in SDK tests library."""

import pytest
from typing import Any, Dict
from unittest.mock import patch

from singer_sdk.testing import get_target_test_class

from target_s3_parquet.target import TargetS3Parquet

SAMPLE_CONFIG: Dict[str, Any] = {
    "s3_path": "s3://mock-bucket/path",
    "athena_database": "mock_database",
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetS3Parquet,
    config=SAMPLE_CONFIG,
)


class TestTargetS3Parquet(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(autouse=True)
    def mock_aws(self):
        with patch("awswrangler.catalog.does_table_exist", return_value=False), patch(
            "awswrangler.catalog.table", return_value=None
        ), patch("awswrangler.s3.to_parquet", return_value=None):
            yield
