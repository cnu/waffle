from collections import defaultdict
import datetime
import pickle
import uuid

import simplejson
import sqlalchemy
from sqlalchemy import sql

class Codec(object):
    def decode(self, bytes):
        raise NotImplementedError("not implemented")

    def encode(self, object):
        raise NotImplementedError("not implemented")

class PickleCodec(Codec):
    def decode(self, bytes):
        return pickle.loads(bytes)

    def encode(self, object):
        return pickle.dumps(bytes)

class JSONCodec(Codec):
    def decode(self, bytes):
        return simplejson.loads(bytes)

    def encode(self, object):
        return simplejson.dumps(object)

class CodecPipeline(Codec):
    def __init__(self, codecs=()):
        self.codecs = list(codecs)

    def encode(self, object):
        for codec in codecs:
            object = codec.encode(object)
        return object

    def decode(self, bytes):
        for codec in reversed(codecs):
            bytes = codec.decode(bytes)
        return bytes

class Entity(object):
    def __init__(self, name, codec=JSONCodec(), engines=None, indices=None, record_class=None):
        self.name = unicode(name)
        self.codec = codec
        self.engines = list(engines)
        self.indices = list(indices) if indices is not None else []
        self.record_class = record_class if record_class is not None else Record
        self.table = sqlalchemy.Table(
                name, 
                sqlalchemy.MetaData(),
                sqlalchemy.Column('increment_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('id', sqlalchemy.Binary(length=16), unique=True, index=True),
                sqlalchemy.Column('payload', sqlalchemy.BLOB()),
                sqlalchemy.Column('updated', sqlalchemy.DATETIME(), onupdate=datetime.datetime.now)
            )

    def create(self):
        for engine in self.engines:
            self.table.metadata.create_all(engine, checkfirst=True)

        for index in self.indices:
            index.create()

    def engine_for_uuid(self, uuid):
        num = len(self.engines)
        return self.engines[uuid.int % num]

    def save_record(self, record):
        """Save a record in the data store
        
        Arguments
        record -- Record, the record to be saved
        """
        engine = self.engine_for_uuid(record.id)
        conn = engine.connect()
        trx = conn.begin()
        assert record.id and record.id.bytes
        result = list(conn.execute(sql.select([self.table], self.table.c.id == record.id.bytes)))
        payload = self.codec.encode(record.value)
        if len(result) == 0:
            conn.execute(self.table.insert(), id=record.id.bytes, payload=payload)
        else:    
            conn.execute(self.table.update()
                    .where(record.c.id == record.id.bytes)
                    .values(payload=payload))
        trx.commit()
        conn.close()
        for index in self.indices:
            index.save_record(record)

    def get_record(self, record_id):
        """Load a record by ID

        Arguments:
        record_id -- UUID, a UUID for the record ID

        Returns:
        Record
        """
        result = list(conn.execute(sql.select([self.table], self.table.c.id == record_id.bytes)))
        return result[0]

    def new(self):
        return self.record_class(entity=self)

class Index(object):
    def __init__(self, index_name, columns, engines, mapping_generator, shard_on=['id'], hash_func=hash):
        self.metadata = sqlalchemy.MetaData()
        table_cols = list(columns) + [sqlalchemy.Column('id', sqlalchemy.Binary(length=16), index=True)]
        self.table = sqlalchemy.Table(index_name, self.metadata, *table_cols)
        self.index = sqlalchemy.Index(index_name + '_val_idx', *columns)
        self.engines = engines
        self.mapping_generator = mapping_generator
        self.hash_func = hash_func
        self.shard_on = list(shard_on)

    def create(self):
        for engine in self.engines:
            self.metadata.create_all(engine, checkfirst=True)

    def hash_mapping(self, record, mapping):
        values = []
        h = 0
        for key in self.shard_on:
            if key == 'id':
                h += record.id.int
            else:
                h += self.hash_func(mapping[key])
        return h

    def save_record(self, record):
        num_engines = len(self.engines)
        idx_to_mapping = defaultdict(list)
        for mapping in self.mapping_generator(record):
            idx_to_mapping[self.hash_mapping(record, mapping) % num_engines].append(mapping)
            
        for i, engine in enumerate(self.engines):
            mappings = idx_to_mapping[i]
            conn = engine.connect()
            trx = conn.begin()
            conn.execute(self.table.delete().where(self.table.c.id == record.id.bytes))
            for m in mappings:
                conn.execute(self.table.insert().values(id=record.id.bytes, **mapping))
            trx.commit()
            conn.close()

class Record(object):
    def __init__(self, id=None, increment_id=None, updated=None, value=None, entity=None): 
        self.id = id if id is not None else uuid.uuid4()
        self.updated = updated
        self.value = value
        self.entity = entity

    def __repr__(self):
        return 'Record(id=%(id)r, updated=%(updated)r, value=%(value)r, table=%(table)r)' % vars(self)

    def save(self):
        self.entity.save_record(self)

__all__ = [
        'Record',
        'Table',
        'Codec',
        'CodecPipeline',
        'JSONCodec',
        'PickleCodec',
        'Table',
        'Index',
        ]
