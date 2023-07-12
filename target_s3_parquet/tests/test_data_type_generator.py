from target_s3_parquet.data_type_generator import (
    generate_tap_schema,
    generate_current_target_schema,
)
import pytest
from pandas import DataFrame


def test_invalid_schema():
    schema = {"someAttribute": {"invalidKey": []}}

    with pytest.raises(Exception, match="Invalid schema format:"):
        generate_tap_schema(schema)


def test_schema_with_all_of():
    schema = {
        "lastModifiedDate": {
            "anyOf": [
                {"type": "string", "format": "date-time"},
                {"type": ["string", "null"]},
            ]
        }
    }

    assert generate_tap_schema(schema) == {"lastModifiedDate": "string"}


def test_generate_tap_schema():
    schema = {
        "vid": {"type": ["null", "string"]},
        "merged_vids": {
            "type": ["null", "array"],
            "items": {"type": ["null", "integer"]},
        },
        "double_values": {
            "type": ["null", "array"],
            "items": {"type": ["null", "number"]},
        },
    }

    expected_result = {
        "vid": "string",
        "merged_vids": "array<int>",
        "double_values": "array<double>",
    }

    assert generate_tap_schema(schema) == expected_result


def test_complex_schema():
    schema = {
        "identity_profiles": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {
                    "deleted_changed_timestamp": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "saved_at_timestamp": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "vid": {"type": ["null", "integer"]},
                    "identities": {
                        "type": ["null", "array"],
                        "items": {
                            "type": ["null", "object"],
                            "properties": {
                                "timestamp": {
                                    "type": ["null", "string"],
                                    "format": "date-time",
                                },
                                "type": {"type": ["null", "string"]},
                                "value": {"type": ["null", "string"]},
                            },
                        },
                    },
                },
            },
        }
    }

    expected_result = {
        "identity_profiles": "array<struct<deleted_changed_timestamp:timestamp, "
        + "saved_at_timestamp:timestamp, vid:int, "
        + "identities:array<struct<timestamp:timestamp, type:string, "
        + "value:string>>>>"
    }

    assert generate_tap_schema(schema) == expected_result


def test_number_type():
    schema = {
        "property_count_events": {
            "type": "object",
            "properties": {"value": {"type": ["null", "number", "string"]}},
        },
        "identities": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {"some_value": {"type": ["null", "number"]}},
            },
        },
    }

    assert generate_tap_schema(schema) == {
        "property_count_events": "struct<value:double>",
        "identities": "array<struct<some_value:double>>",
    }


def test_integer_type():
    schema = {
        "property_count_events": {
            "type": "object",
            "properties": {"value": {"type": ["null", "integer", "string"]}},
        },
        "identities": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {"some_value": {"type": ["null", "integer"]}},
            },
        },
    }

    assert generate_tap_schema(schema) == {
        "property_count_events": "struct<value:int>",
        "identities": "array<struct<some_value:int>>",
    }


def test_sdc_type_translation():
    schema = {
        "_sdc_batched_at": {"type": ["null", "string"], "format": "date-time"},
        "_sdc_received_at": {"type": ["null", "string"], "format": "date-time"},
        "_sdc_extracted_at": {"type": ["null", "string"], "format": "date-time"},
        "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
        "_sdc_sequence": {"type": ["null", "integer"]},
        "_sdc_table_version": {"type": ["null", "integer"]},
    }

    assert generate_tap_schema(schema) == {
        "_sdc_batched_at": "timestamp",
        "_sdc_received_at": "timestamp",
        "_sdc_extracted_at": "timestamp",
        "_sdc_deleted_at": "timestamp",
        "_sdc_sequence": "string",
        "_sdc_table_version": "string",
    }


def test_only_string_definition():
    schema = {
        "property_count_events": {
            "type": "object",
            "properties": {"value": {"type": ["null", "integer", "string"]}},
        },
        "identities": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {"some_value": {"type": ["null", "integer"]}},
            },
        },
    }

    assert generate_tap_schema(schema, only_string=True) == {
        "property_count_events": "string",
        "identities": "string",
    }


def test_binary_type():
    schema = {
        "image": {"type": ["null", "string"], "description": "blob"},
        "free_text": {"type": ["null", "string"], "description": "raw"},
    }

    assert generate_tap_schema(schema) == {"image": "binary", "free_text": "binary"}


def test_singer_decimal_type():
    schema = {"measurement": {"type": ["null", "string"], "format": "singer.decimal"}}

    assert generate_tap_schema(schema) == {"measurement": "double"}


def test_get_current_schema():
    schema = {
        "Column Name": ["identity_profiles", "identities"],
        "Type": ["string", "string"],
        "Partition": [False, False],
        "Comment": ["", ""],
    }
    schema_df = DataFrame(schema)
    assert generate_current_target_schema(schema_df) == {
        "identity_profiles": "string",
        "identities": "string",
    }


def test_get_current_schema_empty():
    schema = {}
    schema_df = DataFrame(schema)
    assert generate_current_target_schema(schema_df) == {}
