from collections import defaultdict
import datetime
import functools
import pickle
import uuid

try:
    import json
except ImportError:
    import simplejson as json

import sqlalchemy
from sqlalchemy import sql
from sqlalchemy.sql import visitors


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

class _JSONEncoder(json.JSONEncoder):
    """JSON encoder that supports encoding UUIDs"""
    def default(self, obj): 
        if isinstance(obj, uuid.UUID):
            return {'__uuid__': True, 'hex': obj.hex}
        else:
            return super(_JSONEncoder, self).default(obj)

class JSONCodec(Codec):
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

class IndexList(list):
    def __getattr__(self, name):
        for index in self:
            if index.name == name:
                return index
        super(IndexList, self).__getattr__(name)

class Entity(object):
    def __init__(self, name, codec=JSONCodec(), engines=None, indices=None, record_class=None):
        self.name = unicode(name)
        self.codec = codec
        self.engines = list(engines)
        self.indices = IndexList(list(indices) if indices is not None else [])
        self.record_class = record_class if record_class is not None else Record
        self.table = sqlalchemy.Table(
                name, 
                sqlalchemy.MetaData(),
                sqlalchemy.Column('increment_id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('id', UUIDColumn(), unique=True, index=True),
                sqlalchemy.Column('payload', PayloadColumn(codec)),
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

    def save(self, record):
        """Save a record in the data store
        
        Arguments
        record -- Record, the record to be saved
        """
        engine = self.engine_for_uuid(record.id)
        conn = engine.connect()
        trx = conn.begin()
        assert record.id and record.id.bytes
        result = list(conn.execute(sql.select([self.table], self.table.c.id == record.id)))
        if len(result) == 0:
            conn.execute(self.table.insert(), id=record.id, payload=record.value)
        else:    
            conn.execute(self.table.update()
                    .where(record.c.id == record.id)
                    .values(payload=record.value))
        trx.commit()
        conn.close()
        for index in self.indices:
            index.save(record)

    def lookup(self, record_ids):
        """Load a record by ID

        Arguments:
        record_id -- UUID, a UUID for the record ID

        Returns:
        Record
        """
        partitions = defaultdict(set)
        num = len(self.engines)
        for record_id in record_ids:
            partitions[self.engines[record_id.int % num]].add(record_id)    

        result = []
        for engine, sub_record_ids in partitions.iteritems():
            conn = engine.connect()
            query = sql.select([self.table], self.table.c.id.in_(sub_record_ids))
            sub_result = conn.execute(query)
            result.extend(sub_result)
        
        records = []
        for r in result:
            rec = Record(id=r.id, value=r.payload, updated=r.updated, increment_id=r.increment_id)
            records.append(rec)
        return records

    def new(self):
        return self.record_class()

    @property 
    def c(self):
        return IndexClauseElement(self)

    def select(self, *clauses):
        if not clauses:
            return []

        clauses = reduce(sqlalchemy.and_, clauses) if clauses else []

        tables = []
        def check_unique_table(column):
            if tables and column.table not in tables:
                raise NotImplementedError("Can't join indices yet")
            tables.append(column.table)
        visitors.traverse(clauses, {}, {'column': functools.partial(check_unique_table)})
        assert tables

        index_vals = []
        for index in self.indices:
            if index.table == tables[0]:
                index_vals.extend(index.select(clauses))
                break
        ids = set(i.id for i in index_vals)
        return self.lookup(ids)

class IndexPartitionStrategy(object):
    """A strategy for sharding index values and queries
    """ 
    def __init__(self, engines):
        self.engines = engines

    def engines_for_record_mapping(self, record, mapping):
        """Get the engine for a record and mapping"""
        raise NotImplementedError("subclasses should implement engine_for_record_mapping")

    def engines_for_clauses(self, clauses):
        """Get all of the engines for a query"""
        return self.engines

class PartitionByPrimaryKey(IndexPartitionStrategy):
    def engine_for_record_mapping(self, record, mapping):
        return self.engines[record.id.int % len(self.engines)]

class Index(object):
    def __init__(self, name, columns, mapper, partition):
        self.name = name
        self.metadata = sqlalchemy.MetaData()
        table_cols = list(columns) + [sqlalchemy.Column('id', UUIDColumn(), index=True)]
        self.table = sqlalchemy.Table(name, self.metadata, *table_cols)
        self.c = self.table.c 
        self.index = sqlalchemy.Index(name + '_val_idx', *columns)
        self.mapper = mapper
        self.partition = partition

    def select(self, clause):
        results = []
        for engine in self.partition.engines_for_clauses(clause):
            conn = engine.connect()
            results_ = conn.execute(sql.select([self.table], clause))
            results.extend(results_)
            conn.close()
        return results

    def create(self):
        for engine in self.partition.engines:
            self.metadata.create_all(engine, checkfirst=True)

    def save(self, record):
        engine_to_mapping = defaultdict(list)
        for mapping in self.mapper(record):
            engine = self.partition.engine_for_record_mapping(record, mapping)
            engine_to_mapping[engine].append(mapping)

        for e in self.partition.engines:
            conn = engine.connect()
            trx = conn.begin()
            conn.execute(self.table.delete().where(self.table.c.id == record.id))
            for mapping in engine_to_mapping[engine]:
                conn.execute(self.table.insert().values(id=record.id, **mapping))
            trx.commit()
            conn.close()

class Record(object):
    def __init__(self, id=None, increment_id=None, updated=None, value=None): 
        self.id = id if id is not None else uuid.uuid4()
        self.updated = updated
        self.value = value

    def __repr__(self):
        return 'Record(id=%(id)r, updated=%(updated)r, value=%(value)r)' % vars(self)

class PayloadColumn(sqlalchemy.types.TypeDecorator):
    impl = sqlalchemy.Binary

    def __init__(self, codec):  
        self.codec = codec
        super(PayloadColumn, self).__init__()

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
        'Record',
        'Table',
        'Codec',
        'PartitionByPrimaryKey',
        'IndexPartitionStrategy',
        'CodecPipeline',
        'JSONCodec',
        'PickleCodec',
        'Table',
        'Index',
        ]
