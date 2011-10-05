## Introduction

`riak_crdt` is a CRDT loader for Riak using the [crdt API](https://github.com/ericmoritz/crdt)

## Usage


    from riak_crdt.loader import Loader
    from crdt.sets import LWWSet
    from riak import RiakClient

    client = RiakClient()
    bucket = client.bucket("friends")

    with Loader(LWWSet, bucket," "eric") as friend_set:
	    friend_set.add("eric")

What just happened?  The loader automattically loaded the riak object at friends/eric and created a LWWSet from the data if it existed or created a new LWWSet if the data did not exist.

If the object had siblings, the loader automattically resolves the conflict using the LWWSet's merge method.
