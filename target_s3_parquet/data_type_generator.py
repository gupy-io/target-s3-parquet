def remove_nulls(array):
    return [v for v in array if v != "null"]


def sanitize_attributes(attributes):
    if isinstance(attributes, list):
        return remove_nulls(attributes)[0]
    else:
        return attributes


def coerce_types(name, type):
    if name == "_sdc_sequence":
        return "string"

    if name == "_sdc_table_version":
        return "string"

    if type == "number":
        return "double"

    if type == "integer":
        return "int"

    return type


def build_struct_type(attributes, level):
    object_data_types = generate_column_schema(attributes, level)

    stringfy_data_types = ", ".join([f"{k}:{v}" for k, v in object_data_types.items()])

    return f"struct<{stringfy_data_types}>"


def generate_column_schema(schema, level=0):
    field_definitions = {}
    new_level = level + 1

    for name, attributes in schema.items():
        cleaned_type = sanitize_attributes(attributes["type"])

        if cleaned_type == "object":
            field_definitions[name] = build_struct_type(
                attributes["properties"], new_level
            )
        elif cleaned_type == "array":
            array_type = sanitize_attributes(attributes["items"]["type"])

            if array_type == "object":
                array_type = build_struct_type(
                    attributes["items"]["properties"], new_level + 1
                )

            array_type = coerce_types(name, array_type)

            field_definitions[name] = f"array<{array_type}>"
        else:
            type = coerce_types(name, cleaned_type)

            field_definitions[name] = type

    return field_definitions
