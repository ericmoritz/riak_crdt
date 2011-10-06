import time
import unittest
import riak
import os
from crdt.sets import LWWSet
from riak_crdt import Loader
import logging

logging.basicConfig(level=logging.DEBUG)

client = riak.RiakClient(port=8091)


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.bucket = client.bucket("test-crdt")
        obj = self.bucket.get("test")
        obj.delete()

    def test_siblings(self):
        self.bucket.set_allow_multiples(True)

        # Create the source object
        loader = Loader(LWWSet, self.bucket, "test")
        loader.obj.add("eric")
        loader.commit()

        # Create two siblings
        loader1 = Loader(LWWSet, self.bucket, "test")
        loader1.obj.add("glenn")

        loader2 = Loader(LWWSet, self.bucket, "test")
        loader2.obj.add("mark")

        loader1.commit()
        loader2.commit()

        loader3 = Loader(LWWSet, self.bucket, "test")
        loader3.load()
        loader3.commit()

        self.assertEqual(loader3.obj.value,
                         set(["glenn", "mark", "eric"]))

    def test_contextmanager(self):
        with Loader(LWWSet, self.bucket, "test") as obj:
            obj.add("eric")
            obj.add("glenn")
            obj.add("foo")

        loader = Loader(LWWSet, self.bucket, "test")
        self.assertEqual(loader.obj.value,
                         set(["eric", "glenn", "foo"]))

    def test_refresh(self):
        loader = Loader(LWWSet, self.bucket, "test")
        # sanity check
        self.assertTrue("eric" not in loader.obj)
        loader.obj.add("eric")

        self.assertTrue("eric" in loader.obj)

        loader.refresh()
        self.assertTrue("eric" not in loader.obj)

    def test_abort(self):
        loader = Loader(LWWSet, self.bucket, "test")
        # sanity check
        self.assertTrue("eric" not in loader.obj)

        with Loader(LWWSet, self.bucket, "test") as obj:
            obj.add("eric")
            Loader.abort()

        # Fetch the object again
        loader.refresh()

        # sanity check
        self.assertTrue("eric" not in loader.obj)
