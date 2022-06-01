from target_s3_parquet.sanitizer import (
    get_valid_types,
    type_from_anyof,
    get_specific_type_attributes,
    apply_json_dump_to_df,
    get_valid_attributes,
)
from pandas import DataFrame
import json


def test_dict_type_transformation():
    context = {
        "records": [
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
                "property_name": "atributo_1",
            },
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
                "property_name": "atributo_1",
            },
        ]
    }
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
        "property_name": {"type": "string"},
    }
    df = DataFrame(context["records"])
    attributes_names = get_specific_type_attributes(schema, "object")
    df_transformed = apply_json_dump_to_df(df, attributes_names)
    assert (
        df[attributes_names].equals(df_transformed[attributes_names]) == False
    ), "object type columns must be transformed"

    assert json.loads(df_transformed.loc[0, "property_count_events"]) == {
        "a": 1,
        "subdomain": "mac donald's",
    }, "transformed columns must be a valid json"

    assert (
        df.loc[:, ~df.columns.isin(attributes_names)].equals(
            df_transformed.loc[:, ~df_transformed.columns.isin(attributes_names)]
        )
        == True
    ), "should only transform object type columns"


def test_should_get_valid_attributes():
    context = {
        "records": [
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
                "property_name": "atributo_1",
            },
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
                "property_name": "atributo_1",
            },
        ]
    }
    df = DataFrame(context["records"])
    attributes_names = ["property_count_events"]
    valid_attributes = get_valid_attributes(attributes_names, df)
    assert valid_attributes == attributes_names


def test_shouldnt_get_valid_attributes():
    context = {
        "records": [
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
            },
            {
                "property_count_events": {"a": 1, "subdomain": "mac donald's"},
                "identities": [{"some_value": 1}],
            },
        ]
    }
    df = DataFrame(context["records"])
    attributes_names = ["property_name"]
    valid_attributes = get_valid_attributes(attributes_names, df)
    assert valid_attributes == []
