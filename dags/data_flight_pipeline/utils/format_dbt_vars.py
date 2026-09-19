import json


def format_dbt_vars(vars_: dict[str, str]) -> str:
    return f"'{json.dumps(vars_)}'"
