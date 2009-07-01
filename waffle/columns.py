"""Contains custom data types for the columns in table

 * BinaryEncodedColumn
 * UUIDColumn
"""

import uuid

import sqlalchemy

class BinaryEncodedColumn(sqlalchemy.types.TypeDecorator):
    """A column for storing data encoded in a Binary format

    Arguments:
    codec -- an instance of Codec, something with a .decode() and .encode()
    """
    impl = sqlalchemy.Binary

    def __init__(self, codec):  
        self.codec = codec
        super(BinaryEncodedColumn, self).__init__()

    def process_bind_param(self, value, dialect=None):
        if value is None:
            return None
        else:
            return self.codec.encode(value)

    def process_result_value(self, value, dialect=None):
        if value is None:
            return None
        else:
            return self.codec.decode(value)

class UUIDColumn(sqlalchemy.types.TypeDecorator):
    """A SQLAlchemy column for storing UUID values"""

    impl = sqlalchemy.Binary

    def __init__(self):
        self.impl.length = 16
        super(UUIDColumn, self).__init__(length=16)
 
    def process_bind_param(self, value, dialect=None):
        if value and isinstance(value, uuid.UUID):
            return value.bytes
        elif value and not isinstance(value, uuid.UUID):
            raise ValueError('value %s is not a valid uuid.UUID' % value)
        else:
            return None
 
    def process_result_value(self, value, dialect=None):
        if value:
            return uuid.UUID(bytes=value)
        else:
            return None
 
    def is_mutable(self):
        return False


__all__ = [
    'BinaryEncodedColumn',
    'UUIDColumn',
    ]
