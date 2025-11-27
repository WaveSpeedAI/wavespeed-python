"""Input validation utilities for the serverless worker.

Compatible with RunPod's rp_validator.
"""

import json
from typing import Any, Dict, List, Union

# Error messages
UNEXPECTED_INPUT_ERROR = "Unexpected input. {} is not a valid input option."
MISSING_REQUIRED_ERROR = "{} is a required input."
MISSING_DEFAULT_ERROR = "Schema error, missing default value for {}."
MISSING_TYPE_ERROR = "Schema error, missing type for {}."
INVALID_TYPE_ERROR = "{} should be {} type, not {}."
CONSTRAINTS_ERROR = "{} does not meet the constraints."
SCHEMA_ERROR = "Schema error, {} is not a dictionary."


def _add_error(error_list: List[str], message: str) -> None:
    """Add an error message to the error list."""
    error_list.append(message)


def _check_for_unexpected_inputs(
    raw_input: Dict[str, Any],
    schema: Dict[str, Any],
    error_list: List[str],
) -> None:
    """Check for inputs not defined in schema."""
    for key in raw_input:
        if key not in schema:
            _add_error(error_list, UNEXPECTED_INPUT_ERROR.format(key))


def _validate_and_transform_schema_items(
    schema: Dict[str, Any],
    error_list: List[str],
) -> None:
    """Validate schema items and transform JSON strings to dicts."""
    for key, rules in schema.items():
        if not isinstance(rules, dict):
            try:
                schema[key] = json.loads(rules)
            except json.decoder.JSONDecodeError:
                _add_error(error_list, SCHEMA_ERROR.format(key))


def _validate_required_inputs_and_set_defaults(
    raw_input: Dict[str, Any],
    schema: Dict[str, Any],
    validated_input: Dict[str, Any],
    error_list: List[str],
) -> None:
    """Validate required inputs and set defaults."""
    for key, rules in schema.items():
        if "type" not in rules:
            _add_error(error_list, MISSING_TYPE_ERROR.format(key))

        if "required" not in rules:
            _add_error(error_list, MISSING_REQUIRED_ERROR.format(key))
        elif rules["required"] and key not in raw_input:
            _add_error(error_list, MISSING_REQUIRED_ERROR.format(key))
        elif not rules["required"] and key not in raw_input:
            if "default" in rules:
                validated_input[key] = rules["default"]
            else:
                _add_error(error_list, MISSING_DEFAULT_ERROR.format(key))


def _validate_input_against_schema(
    schema: Dict[str, Any],
    validated_input: Dict[str, Any],
    error_list: List[str],
) -> None:
    """Validate input values against schema types and constraints."""
    for key, rules in schema.items():
        if key in validated_input:
            # Skip if type is not defined in schema
            if "type" not in rules:
                continue

            # Enforce floats to be floats (int -> float conversion only)
            try:
                if rules["type"] is float and type(validated_input[key]) in [
                    int,
                    float,
                ]:
                    validated_input[key] = float(validated_input[key])
            except TypeError:
                continue

            # Check for the correct type
            try:
                is_instance = isinstance(validated_input[key], rules["type"])
            except TypeError:
                # type is not a valid type (e.g., string from JSON parsing)
                is_instance = False

            if not is_instance:
                _add_error(
                    error_list,
                    f"{key} should be {rules['type']} type, not {type(validated_input[key])}.",
                )

        # Check lambda constraints
        if "constraints" in rules and not rules["constraints"](
            validated_input.get(key)
        ):
            _add_error(error_list, CONSTRAINTS_ERROR.format(key))


def validate(
    raw_input: Dict[str, Any],
    schema: Dict[str, Any],
) -> Dict[str, Union[Dict[str, Any], List[str]]]:
    """Validate input against a schema.

    Validates the input:
    - Checks if provided inputs match expected types
    - Checks if required inputs are included
    - Sets default values for inputs not provided
    - Validates inputs using lambda constraints

    Args:
        raw_input: The raw input dictionary to validate.
        schema: The validation schema. Each key maps to a dict with:
            - type: The expected type (required)
            - required: Whether the field is required (required)
            - default: Default value if not required and not provided
            - constraints: Lambda function for additional validation

    Returns:
        Either {"errors": ["error1", "error2"]} if validation fails,
        or {"validated_input": {"input1": "value1"}} if successful.

    Example:
        schema = {
            "prompt": {
                "type": str,
                "required": True,
            },
            "max_tokens": {
                "type": int,
                "required": False,
                "default": 100,
            },
            "temperature": {
                "type": float,
                "required": False,
                "default": 0.7,
                "constraints": lambda x: 0 <= x <= 2,
            },
        }
        result = validate(job_input, schema)
        if "errors" in result:
            print(result["errors"])
        else:
            validated = result["validated_input"]
    """
    error_list: List[str] = []
    validated_input = raw_input.copy()

    _check_for_unexpected_inputs(raw_input, schema, error_list)
    _validate_and_transform_schema_items(schema, error_list)
    _validate_required_inputs_and_set_defaults(
        raw_input, schema, validated_input, error_list
    )
    _validate_input_against_schema(schema, validated_input, error_list)

    if error_list:
        return {"errors": error_list}

    return {"validated_input": validated_input}
