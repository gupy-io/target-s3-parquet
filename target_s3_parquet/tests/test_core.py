"""Tests standard target features using the built-in SDK tests library."""

from typing import Any, Dict

from singer_sdk.testing import get_standard_target_tests

from target_s3_parquet.target import TargetS3Parquet

SAMPLE_CONFIG: Dict[str, Any] = {
    "s3_path": "s3://unit-test-bucket/target-s3-parquet",
    "athena_database": "target_s3_parquet_test",
}


# Run standard built-in target tests from the SDK:
def test_standard_target_tests():
    """Run standard target tests from the SDK."""
    tests = get_standard_target_tests(
        TargetS3Parquet,
        config=SAMPLE_CONFIG,
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your target.
