"""Tests for the validator module."""

import unittest

from wavespeed.serverless.utils.validator import validate


class TestValidateBasic(unittest.TestCase):
    """Basic tests for the validate function."""

    def test_valid_input(self):
        """Test validation with valid input."""
        schema = {
            "name": {"type": str, "required": True},
            "age": {"type": int, "required": False, "default": 0},
        }
        raw_input = {"name": "John", "age": 30}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["name"], "John")
        self.assertEqual(result["validated_input"]["age"], 30)

    def test_missing_required_field(self):
        """Test validation with missing required field."""
        schema = {
            "prompt": {"type": str, "required": True},
        }
        raw_input = {}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("prompt" in err for err in result["errors"]))

    def test_default_value_applied(self):
        """Test that default values are applied."""
        schema = {
            "temperature": {"type": float, "required": False, "default": 0.7},
        }
        raw_input = {}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["temperature"], 0.7)

    def test_unexpected_input_error(self):
        """Test that unexpected inputs generate errors."""
        schema = {
            "name": {"type": str, "required": True},
        }
        raw_input = {"name": "John", "extra": "field"}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("extra" in err for err in result["errors"]))


class TestValidateTypeChecking(unittest.TestCase):
    """Tests for type checking in validate."""

    def test_correct_type(self):
        """Test validation passes with correct types."""
        schema = {
            "count": {"type": int, "required": True},
        }
        raw_input = {"count": 42}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["count"], 42)

    def test_wrong_type_error(self):
        """Test validation fails with wrong type."""
        schema = {
            "count": {"type": int, "required": True},
        }
        raw_input = {"count": "not an int"}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(
            any("count" in err and "type" in err for err in result["errors"])
        )

    def test_int_to_float_conversion(self):
        """Test that int is converted to float when float is expected."""
        schema = {
            "score": {"type": float, "required": True},
        }
        raw_input = {"score": 5}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["score"], 5.0)
        self.assertIsInstance(result["validated_input"]["score"], float)

    def test_no_string_to_int_conversion(self):
        """Test that string is NOT converted to int (unlike old behavior)."""
        schema = {
            "count": {"type": int, "required": True},
        }
        raw_input = {"count": "42"}

        result = validate(raw_input, schema)

        # Should fail - no auto-conversion from string to int
        self.assertIn("errors", result)


class TestValidateSchemaErrors(unittest.TestCase):
    """Tests for schema validation errors."""

    def test_missing_type_in_schema(self):
        """Test error when schema is missing type."""
        schema = {
            "field": {"required": True},  # missing type
        }
        raw_input = {"field": "value"}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("type" in err.lower() for err in result["errors"]))

    def test_missing_required_in_schema(self):
        """Test error when schema is missing required."""
        schema = {
            "field": {"type": str},  # missing required
        }
        raw_input = {"field": "value"}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("required" in err.lower() for err in result["errors"]))

    def test_missing_default_for_optional(self):
        """Test error when optional field has no default."""
        schema = {
            "field": {"type": str, "required": False},  # no default
        }
        raw_input = {}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("default" in err.lower() for err in result["errors"]))


class TestValidateLambdaConstraints(unittest.TestCase):
    """Tests for lambda constraint validation."""

    def test_constraint_passes(self):
        """Test validation passes when constraint is satisfied."""
        schema = {
            "temperature": {
                "type": float,
                "required": True,
                "constraints": lambda x: 0 <= x <= 2,
            },
        }
        raw_input = {"temperature": 1.0}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["temperature"], 1.0)

    def test_constraint_fails(self):
        """Test validation fails when constraint is not satisfied."""
        schema = {
            "temperature": {
                "type": float,
                "required": True,
                "constraints": lambda x: 0 <= x <= 2,
            },
        }
        raw_input = {"temperature": 5.0}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        self.assertTrue(any("constraints" in err.lower() for err in result["errors"]))

    def test_constraint_with_default_value(self):
        """Test constraint behavior with default value."""
        schema = {
            "value": {
                "type": int,
                "required": False,
                "default": 10,
                "constraints": lambda x: x > 0,
            },
        }
        raw_input = {}

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        self.assertEqual(result["validated_input"]["value"], 10)


class TestValidateJsonSchema(unittest.TestCase):
    """Tests for JSON schema transformation."""

    def test_json_string_schema(self):
        """Test that JSON string schema items are parsed."""
        schema = {
            "field": '{"type": "str", "required": true}',
        }
        raw_input = {"field": "value"}

        # This should parse the JSON string
        result = validate(raw_input, schema)

        # Note: After JSON parsing, "str" is a string, not the str type
        # This will likely fail type check, but schema should be parsed
        self.assertIn("errors", result)  # type mismatch expected


class TestValidateIntegration(unittest.TestCase):
    """Integration tests for validate function."""

    def test_complex_schema(self):
        """Test validation with a complex schema."""
        schema = {
            "prompt": {
                "type": str,
                "required": True,
                "constraints": lambda x: len(x) > 0,
            },
            "max_tokens": {
                "type": int,
                "required": False,
                "default": 100,
                "constraints": lambda x: 1 <= x <= 4096,
            },
            "temperature": {
                "type": float,
                "required": False,
                "default": 0.7,
                "constraints": lambda x: 0 <= x <= 2,
            },
        }

        raw_input = {
            "prompt": "Hello, world!",
            "max_tokens": 500,
        }

        result = validate(raw_input, schema)

        self.assertIn("validated_input", result)
        validated = result["validated_input"]
        self.assertEqual(validated["prompt"], "Hello, world!")
        self.assertEqual(validated["max_tokens"], 500)
        self.assertEqual(validated["temperature"], 0.7)

    def test_multiple_errors(self):
        """Test that multiple errors are collected."""
        schema = {
            "field1": {"type": str, "required": True},
            "field2": {"type": int, "required": True},
        }
        raw_input = {"extra1": "x", "extra2": "y"}

        result = validate(raw_input, schema)

        self.assertIn("errors", result)
        # Should have errors for: extra1, extra2, field1 required, field2 required
        self.assertGreaterEqual(len(result["errors"]), 4)


if __name__ == "__main__":
    unittest.main()
