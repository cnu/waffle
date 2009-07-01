"""Contains the codecs for serializing the python objects

 * PickleCodec
 * JSONCodec
"""

import pickle
import uuid

try:
    import json
except ImportError:
    import simplejson as json

class Codec(object):
    """A codec is a helper class to serialize Python objects into and out of
    the database in the .body values
    """
    def decode(self, bytes):
        """Decode a byte string into a Python object"""
        raise NotImplementedError("not implemented")

    def encode(self, object):
        """Encode a Python object into a byte string."""
        raise NotImplementedError("not implemented")

class PickleCodec(Codec):
    """A codec for storing objects in the database using Python's Pickle protocol
    """
    def decode(self, bytes):
        return pickle.loads(bytes)

    def encode(self, object):
        return pickle.dumps(bytes)

class _JSONEncoder(json.JSONEncoder):
    """JSON encoder that supports encoding UUIDs"""
    def default(self, obj): 
        if isinstance(obj, uuid.UUID):
            return {'__uuid__': True, 'hex': obj.hex}
        else:
            return super(_JSONEncoder, self).default(obj)

class JSONCodec(Codec):
    """A codec for storing objects in the database using JSON.

    In this case Python's standard JSON codec is extended to support encoding UUID's into JSON like 
    '{__uuid__: True, hex: "98d436da-c3ca-4db4-a05f-fbcb10b84313"}'
    """

    def _decode_object_hook(self, obj):
        if obj.get('__uuid__'):
            hex = obj.get('hex')
            if hex is not None:
                return uuid.UUID(hex=hex)
            num = obj.get('int') 
            if num is not None:
                return uuid.UUID(int=num)
        return obj

    def _encode_hook(self, obj):
        if isinstance(obj, uuid.UUID):
            return {'__uuid__': True, 'hex': obj.hex}
        else:
            return json.JSONEncoder().default(obj)

    def decode(self, bytes):
        return json.loads(str(bytes), object_hook=self._decode_object_hook)

    def encode(self, object):
        s = json.dumps(object, default=self._encode_hook)
        return s
__all__ = [
        'Codec',
        'JSONCodec',
        'PickleCodec',
        ]
