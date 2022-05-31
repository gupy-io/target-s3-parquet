def remove_nulls(array):
    return [v for v in array if v != "null"]

def get_valid_types(types):
    if isinstance(types, list):
        return remove_nulls(types)[0]
    else:
        return types

def type_from_anyof(attributes):
    return attributes.get("anyOf") and attributes.get("anyOf")[0].get("type")