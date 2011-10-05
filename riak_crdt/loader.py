
class Loader(object):
    def __init__(self, crdt_class, bucket, key,
                 content_type="application/json"):
        self.bucket = bucket
        self.key = key
        self.crdt_class = crdt_class
        self._crdt_obj = None
        self._riak_obj = None
        self.content_type = content_type

    @property
    def obj(self):
        if self._crdt_obj is None:
            self.load()

        # Return the cached crdt object
        return self._crdt_obj

    def load(self):
        riak_obj = self.bucket.get(self.key)

        if riak_obj.has_siblings():
            crdt_obj = self.merge(riak_obj)
        elif riak_obj.exists():
            payload = riak_obj.get_data()
            crdt_obj = self.crdt_class.from_payload(payload)
        else:
            crdt_obj = self.crdt_class()

        # Cache the riak object and crdt objects
        self._riak_obj = riak_obj
        self._crdt_obj = crdt_obj
        
    def reset(self):
        self._crdt_obj = self.crdt_class()

    def reload(self):
        self.load()
        return self._crdt_obj
            
    def merge(self, riak_obj):
        siblings = riak_obj.get_siblings()
        first = siblings[0]
        others = siblings[1:]

        sibling_payloads = (obj.get_data() for obj in others)
        sibling_crdts = (self.crdt_class.from_payload(p) \
                             for p in sibling_payloads)

        crdt = self.crdt_class.from_payload(first.get_data())
        for other in sibling_crdts:
            crdt = self.crdt_class.merge(crdt, other)

        return crdt

    def commit(self):
        # Can't commit unloaded data
        if self._crdt_obj is not None:
            payload = self._crdt_obj.payload

            self._riak_obj.set_data(payload)
            self._riak_obj.set_content_type(self.content_type)
            self._riak_obj.store()

            # Clear the cache
            self._riak_obj = None
            self._crdt_obj = None

    def __enter__(self):
        return self.obj

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.commit()
