# Code from https://github.com/transferwise/pipelinewise-target-snowflake

import target_s3_parquet.flattening as flattening

import pytest


def test_flatten_schema():
    """Test flattening of SCHEMA messages"""
    flatten_schema = flattening.flatten_schema

    # Schema with no object properties should be empty dict
    schema_with_no_properties = {"type": "object"}
    assert flatten_schema(schema_with_no_properties) == {}

    not_nested_schema = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
        },
    }

    # NO FLATTENING - Schema with simple properties should be a plain dictionary
    assert flatten_schema(not_nested_schema) == not_nested_schema["properties"]

    nested_schema_with_no_properties = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
            "c_obj": {"type": ["null", "object"]},
        },
    }

    # NO FLATTENING - Schema with object type property but without further
    #                 properties should be a plain dictionary
    assert (
        flatten_schema(nested_schema_with_no_properties)
        == nested_schema_with_no_properties["properties"]
    )

    nested_schema_with_properties = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
            "c_obj": {
                "type": ["null", "object"],
                "properties": {
                    "nested_prop1": {"type": ["null", "string"]},
                    "nested_prop2": {"type": ["null", "string"]},
                    "nested_prop3": {
                        "type": ["null", "object"],
                        "properties": {
                            "multi_nested_prop1": {"type": ["null", "string"]},
                            "multi_nested_prop2": {"type": ["null", "string"]},
                        },
                    },
                },
            },
        },
    }

    # NO FLATTENING - Schema with object type property but without further
    #                 properties should be a plain dictionary
    # No flattening (default)
    assert (
        flatten_schema(nested_schema_with_properties)
        == nested_schema_with_properties["properties"]
    )

    # NO FLATTENING - Schema with object type property but without further
    #                 properties should be a plain dictionary
    #   max_level: 0 : No flattening (default)
    assert (
        flatten_schema(nested_schema_with_properties, max_level=0)
        == nested_schema_with_properties["properties"]
    )

    # FLATTENING - Schema with object type property but without further properties should be a dict with
    # flattened properties
    assert flatten_schema(nested_schema_with_properties, max_level=1) == {
        "c_pk": {"type": ["null", "integer"]},
        "c_varchar": {"type": ["null", "string"]},
        "c_int": {"type": ["null", "integer"]},
        "c_obj__nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop2": {"type": ["null", "string"]},
        "c_obj__nested_prop3": {
            "type": ["null", "object"],
            "properties": {
                "multi_nested_prop1": {"type": ["null", "string"]},
                "multi_nested_prop2": {"type": ["null", "string"]},
            },
        },
    }

    # FLATTENING - Schema with object type property but without further properties should be a dict with
    # flattened properties
    assert flatten_schema(nested_schema_with_properties, max_level=10) == {
        "c_pk": {"type": ["null", "integer"]},
        "c_varchar": {"type": ["null", "string"]},
        "c_int": {"type": ["null", "integer"]},
        "c_obj__nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop2": {"type": ["null", "string"]},
        "c_obj__nested_prop3__multi_nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop3__multi_nested_prop2": {"type": ["null", "string"]},
    }


def test_flatten_record():
    """Test flattening of RECORD messages"""
    flatten_record = flattening.flatten_record

    empty_record = {}
    # Empty record should be empty dict
    assert flatten_record(empty_record) == {}

    not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}

    # NO FLATTENING - Record with simple properties should be a plain dictionary
    assert flatten_record(not_nested_record) == not_nested_record

    nested_record = {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": {
            "nested_prop1": "value_1",
            "nested_prop2": "value_2",
            "nested_prop3": {
                "multi_nested_prop1": "multi_value_1",
                "multi_nested_prop2": "multi_value_2",
            },
        },
    }

    # NO FLATTENING - No flattening (default)
    assert flatten_record(nested_record) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
        '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}',
    }

    # NO FLATTENING
    #   max_level: 0 : No flattening (default)
    assert flatten_record(nested_record, max_level=0) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
        '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}',
    }

    # SEMI FLATTENING
    #   max_level: 1 : Semi-flattening (default)
    assert flatten_record(nested_record, max_level=1) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj__nested_prop1": "value_1",
        "c_obj__nested_prop2": "value_2",
        "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": '
        '"multi_value_2"}',
    }

    # FLATTENING
    assert flatten_record(nested_record, max_level=10) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj__nested_prop1": "value_1",
        "c_obj__nested_prop2": "value_2",
        "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
        "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2",
    }


@pytest.mark.parametrize(
    "should_use_flatten_schema,record,expected_output",
    [
        pytest.param(
            True,
            {"id": 1, "data": "xyz"},
            {"id": "1", "data": "xyz"},
        ),
        pytest.param(
            False,
            {"id": 1, "data": "xyz"},
            {"id": 1, "data": "xyz"},
        ),
    ],
)
def test_flatten_record_with_flatten_schema(
    should_use_flatten_schema, record, expected_output
):
    flatten_record = flattening.flatten_record

    flatten_schema = {"id": {"type": ["object", "array", "null"]}}

    output = flatten_record(
        record, flatten_schema if should_use_flatten_schema else None
    )

    assert output == expected_output


def test_flatten():
    schema = {
        "type": "object",
        "properties": {
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
        },
    }

    flatten_schema = flattening.flatten_schema
    print(flatten_schema(schema))
    assert flatten_schema(schema) == {"foo"}
