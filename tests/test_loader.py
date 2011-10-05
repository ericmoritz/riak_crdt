import time
import unittest
import riak
import os
from crdt.sets import LWWSet
from riak_crdt.loader import Loader

client = riak.RiakClient(transport_class=riak.RiakPbcTransport,
                         port=8081)

class TestLoader(unittest.TestCase):
    def setUp(self):
        super(TestLoader, self).tearDown()

    def tearDown(self):
        bucket = client.bucket("test")
        obj = bucket.get("test")
        obj.delete()
        
    def test_siblings(self):
        bucket = client.bucket("test")
        bucket.set_allow_multiples(True)

        # Create the source object
        loader = Loader(LWWSet, bucket, "test")
        loader.obj.add("eric")
        loader.commit()

        # Create two siblings
        loader1 = Loader(LWWSet, bucket, "test")
        loader1.obj.add("glenn")

        loader2 = Loader(LWWSet, bucket, "test")
        loader2.obj.add("mark")


        loader1.commit()
        loader2.commit()

        loader3 = Loader(LWWSet, bucket, "test")

        self.assertEqual(loader3.obj.value, 
                         set(["glenn", "mark", "eric"]))

    def test_contextmanager(self):
        bucket = client.bucket("test")

        with Loader(LWWSet, bucket, "test") as obj:
            obj.add("eric")
            obj.add("glenn")
            obj.add("foo")
            
        loader = Loader(LWWSet, bucket, "test")
        self.assertEqual(loader.obj.value,
                         set(["eric", "glenn", "foo"]))
