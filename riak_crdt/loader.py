

class Abort(Exception):
    pass


class Loader(object):
    def __init__(self, crdt_class, bucket, key,
                 content_type="application/json"):
        self.bucket = bucket
        self.key = key
        self.crdt_class = crdt_class
        self.content_type = content_type

        # Internal caches
        self._crdt_obj = None
        self._riak_obj = None

    @property
    def obj(self):
        if self._crdt_obj is None:
            self.load()

        return self._crdt_obj

    def load(self):
        riak_obj = self.bucket.get(self.key)

        if riak_obj.has_siblings():
            riak_obj, crdt_obj = self.merge(riak_obj)
        else:
            if riak_obj.exists():
                payload = riak_obj.get_data()
            else:
                payload = ""

            if payload:
                crdt_obj = self.crdt_class.from_payload(payload)
            else:
                crdt_obj = self.crdt_class()

        # Cache the riak object and crdt objects
        self._riak_obj = riak_obj
        self._crdt_obj = crdt_obj

    def refresh(self):
        self.load()
        return self._crdt_obj

    def merge(self, riak_obj):
        siblings = riak_obj.get_siblings()
        #
        # In Riak 1.0, tombstone siblings comeback with no data so we need
        # to filter those out.  This occurs on concurrent DELETE and PUTs
        # I am not sure
        siblings = [obj for obj in siblings
                    if obj.get_data() != ""]

        first = siblings[0]
        others = siblings[1:]

        # Create the starting point using the first sibling
        crdt = self.crdt_class.from_payload(first.get_data())

        # Merge the other siblings to the first one
        for other in others:
            other_crdt = self.crdt_class.from_payload(other.get_data())
            crdt = self.crdt_class.merge(crdt, other_crdt)

        # Return one of the siblings because they have the VClock value
        return first, crdt

    def commit(self):
        """Stores the CRDT into Riak"""
        # Can't commit unloaded data
        if self._crdt_obj is not None:
            self._riak_obj.set_data(self._crdt_obj.payload)
            #
            # Sometimes the content_type isn't set for a source object
            # for instance on 404s
            self._riak_obj.set_content_type(self.content_type)
            #
            self._riak_obj.store()
            #
            # Clear the cache so that the next access to the obj
            # property will result in a reload
            self._riak_obj = None
            self._crdt_obj = None

    @classmethod
    def abort(cls):
        """Aborts a with block batch operation:

        with Loader(LWWSet, bucket, "foo") as obj:
            # If "eric" is not in the set, abort the batch
            if "eric" not in obj:
                Loader.abort()
            else:
                # otherwise, add my parents
                obj.add("eric")
        """
        raise Abort()

    def __enter__(self):
        return self.obj

    def __exit__(self, exc_type, exc_value, traceback):
        # Swallow up aborts without committing
        if exc_type is Abort:
            return True

        # If no exception happened within the with block, commit the
        # object
        if exc_type is None:
            self.commit()

        # Otherwise, return False which tells Python to reraise any exceptions
        return False
