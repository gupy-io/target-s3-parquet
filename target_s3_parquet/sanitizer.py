from pandas import DataFrame
import json
from typing import List


def _remove_nulls(array):
    return [v for v in array if v != "null"]


def get_valid_types(types):
    if isinstance(types, list):
        return _remove_nulls(types)[0]
    else:
        return types


def type_from_anyof(attributes):
    return attributes.get("anyOf") and attributes.get("anyOf")[0].get("type")


def get_specific_type_attributes(schema: dict, attr_type: str) -> list:
    attributes_names = []
    for name, attributes in schema.items():
        attribute_type = attributes.get("type") or type_from_anyof(attributes)
        # if attribute_type is None:
        #     raise Exception(f"Invalid schema format: {schema}")
        cleaned_type = get_valid_types(attribute_type)
        if cleaned_type == attr_type:
            attributes_names.append(name)
    return attributes_names


def apply_json_dump_to_df(
    source_df: DataFrame, attributes_names: List[str]
) -> DataFrame:
    df = source_df.copy()
    if len(attributes_names) > 0:
        for attribute in attributes_names:
            df.loc[:, attribute] = df[attribute].apply(lambda x: json.dumps(x))
    return df
