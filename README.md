## Introduction

`riak_crdt` is a CRDT loader for Riak using the [crdt
API](https://github.com/ericmoritz/crdt)

## Usage

    from riak_crdt.loader import Loader
    from crdt.sets import LWWSet
    from riak import RiakClient

    client = RiakClient()
    bucket = client.bucket("friends")

    with Loader(LWWSet, bucket," "eric") as (loader, friend_set):
	    friend_set.add("tom")

### What just happened?  

1. The loader fetched the riak object at friends/eric
1. If the object did not exist, a new LWWSet is created
1. If the object did exist but had siblings, the LWWSet.merge method
   resolves the conflict.
1. Otherwise, a LWWSet is used using the riak object's payload
1. After the with block concludes, the Loader stores the object

Refer to the [crdt
README](https://github.com/ericmoritz/crdt/blob/master/README.md)
for more details.
