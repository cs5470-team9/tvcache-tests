"""Pure observer tests: no sockets, cache, or cancellation implementation mock."""

import asyncio
import unittest

from tvcache_checks.lifecycle_cases import _DecodeCheckpoint


class DecodeCheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_observes_real_read_and_passes_bytes_unchanged(self):
        reader = asyncio.StreamReader()
        checkpoint = _DecodeCheckpoint(reader)
        read = asyncio.create_task(checkpoint.readexactly(4))
        await checkpoint.entered.wait()
        self.assertFalse(read.done())
        reader.feed_data(b"wire-tail")
        self.assertEqual(await read, b"wire")
        self.assertEqual(await checkpoint.readexactly(5), b"-tail")

    async def test_cancellation_does_not_invent_or_drain_a_response(self):
        reader = asyncio.StreamReader()
        checkpoint = _DecodeCheckpoint(reader)
        read = asyncio.create_task(checkpoint.readexactly(4))
        await checkpoint.entered.wait()
        read.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await read
        reader.feed_data(b"late")
        self.assertEqual(await reader.readexactly(4), b"late")

    async def test_attributes_forward_to_the_real_reader(self):
        reader = asyncio.StreamReader()
        checkpoint = _DecodeCheckpoint(reader)
        self.assertFalse(checkpoint.at_eof())
        reader.feed_eof()
        self.assertTrue(checkpoint.at_eof())
