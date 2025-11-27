"""Unit tests for the handler module."""

import unittest

from wavespeed.serverless.modules.handler import (
    get_handler_type,
    is_async,
    is_async_generator,
    is_generator,
    is_sync_generator,
)


class TestIsGenerator(unittest.TestCase):
    """Tests for the is_generator function."""

    def test_regular_function(self):
        """Test that a regular function is not a generator."""

        def regular_func():
            return "I'm a regular function!"

        self.assertFalse(is_generator(regular_func))

    def test_generator_function(self):
        """Test that a generator function is a generator."""

        def generator_func():
            yield "I'm a generator function!"

        self.assertTrue(is_generator(generator_func))

    def test_async_function(self):
        """Test that an async function is not a generator."""

        async def async_func():
            return "I'm an async function!"

        self.assertFalse(is_generator(async_func))

    def test_async_generator_function(self):
        """Test that an async generator function is a generator."""

        async def async_gen_func():
            yield "I'm an async generator function!"

        self.assertTrue(is_generator(async_gen_func))


class TestIsAsync(unittest.TestCase):
    """Tests for the is_async function."""

    def test_regular_function(self):
        """Test that a regular function is not async."""

        def regular_func():
            return "I'm a regular function!"

        self.assertFalse(is_async(regular_func))

    def test_generator_function(self):
        """Test that a sync generator is not async."""

        def generator_func():
            yield "I'm a generator function!"

        self.assertFalse(is_async(generator_func))

    def test_async_function(self):
        """Test that an async function is async."""

        async def async_func():
            return "I'm an async function!"

        self.assertTrue(is_async(async_func))

    def test_async_generator_function(self):
        """Test that an async generator function is async."""

        async def async_gen_func():
            yield "I'm an async generator function!"

        self.assertTrue(is_async(async_gen_func))


class TestIsAsyncGenerator(unittest.TestCase):
    """Tests for the is_async_generator function."""

    def test_regular_function(self):
        """Test that a regular function is not an async generator."""

        def regular_func():
            return "regular"

        self.assertFalse(is_async_generator(regular_func))

    def test_sync_generator(self):
        """Test that a sync generator is not an async generator."""

        def sync_gen():
            yield "sync"

        self.assertFalse(is_async_generator(sync_gen))

    def test_async_function(self):
        """Test that an async function is not an async generator."""

        async def async_func():
            return "async"

        self.assertFalse(is_async_generator(async_func))

    def test_async_generator(self):
        """Test that an async generator is an async generator."""

        async def async_gen():
            yield "async gen"

        self.assertTrue(is_async_generator(async_gen))


class TestIsSyncGenerator(unittest.TestCase):
    """Tests for the is_sync_generator function."""

    def test_regular_function(self):
        """Test that a regular function is not a sync generator."""

        def regular_func():
            return "regular"

        self.assertFalse(is_sync_generator(regular_func))

    def test_sync_generator(self):
        """Test that a sync generator is a sync generator."""

        def sync_gen():
            yield "sync"

        self.assertTrue(is_sync_generator(sync_gen))

    def test_async_function(self):
        """Test that an async function is not a sync generator."""

        async def async_func():
            return "async"

        self.assertFalse(is_sync_generator(async_func))

    def test_async_generator(self):
        """Test that an async generator is not a sync generator."""

        async def async_gen():
            yield "async gen"

        self.assertFalse(is_sync_generator(async_gen))


class TestGetHandlerType(unittest.TestCase):
    """Tests for the get_handler_type function."""

    def test_sync_function(self):
        """Test sync function type detection."""

        def sync_func():
            return "sync"

        self.assertEqual(get_handler_type(sync_func), "sync")

    def test_async_function(self):
        """Test async function type detection."""

        async def async_func():
            return "async"

        self.assertEqual(get_handler_type(async_func), "async")

    def test_sync_generator(self):
        """Test sync generator type detection."""

        def sync_gen():
            yield "sync gen"

        self.assertEqual(get_handler_type(sync_gen), "sync_generator")

    def test_async_generator(self):
        """Test async generator type detection."""

        async def async_gen():
            yield "async gen"

        self.assertEqual(get_handler_type(async_gen), "async_generator")


if __name__ == "__main__":
    unittest.main()
