from target_s3_parquet.data_type_generator import generate_column_schema
import pytest


def test_invalid_schema():
    schema = {"someAttribute": {"invalidKey": []}}

    with pytest.raises(Exception, match="Invalid schema format:"):
        generate_column_schema(schema)


def test_schema_with_all_of():
    schema = {
        "lastModifiedDate": {
            "anyOf": [
                {"type": "string", "format": "date-time"},
                {"type": ["string", "null"]},
            ]
        },
    }

    assert generate_column_schema(schema) == {"lastModifiedDate": "string"}


def test_generate_column_schema():
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

    assert generate_column_schema(schema) == expected_result


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
        "identity_profiles": "array<struct<deleted_changed_timestamp:string, "
        + "saved_at_timestamp:string, vid:int, "
        + "identities:array<struct<timestamp:string, type:string, "
        + "value:string>>>>",
    }

    assert generate_column_schema(schema) == expected_result


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
                "properties": {
                    "some_value": {
                        "type": ["null", "number"],
                    },
                },
            },
        },
    }

    assert generate_column_schema(schema) == {
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
                "properties": {
                    "some_value": {
                        "type": ["null", "integer"],
                    },
                },
            },
        },
    }

    assert generate_column_schema(schema) == {
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

    assert generate_column_schema(schema) == {
        "_sdc_batched_at": "string",
        "_sdc_received_at": "string",
        "_sdc_extracted_at": "string",
        "_sdc_deleted_at": "string",
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
                "properties": {
                    "some_value": {
                        "type": ["null", "integer"],
                    },
                },
            },
        },
    }

    assert generate_column_schema(schema, only_string=True) == {
        "property_count_events": "string",
        "identities": "string",
    }