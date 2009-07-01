"""waffle

- A library for storing schema-less/document oriented data in relational/SQL databases.
- Grid oriented food
"""
from collections import defaultdict
import datetime
import functools
import uuid

import sqlalchemy
from sqlalchemy import sql
from sqlalchemy.sql import visitors

from codecs import *
from shardstrategy import *

class IndexList(list):
    """A helper class to Entity to provide by name lookup for indices... 
    
    For example:
    "foo_entity.indices.bar_idx.c.bar_id = 5"
    """
    def __getattr__(self, name):
        for index in self:
            if index.name == name:
                return index
        super(IndexList, self).__getattr__(name)

class Entity(object):
    """This is the main table / datastore manager datatype.

    Arguments: 
    name -- string, the SQL table name for the entity /  e.g. An entity which
        stored users would be named "user"
    codec -- waffle.Codec instance; this is used to serialize Record.value.
        Defaults to JSONCodec()
    engines -- [SQLAlchemy engines], this is a list of SQLALchemy engines.  See
        "sqlalchemy.create_engine()"
    indices -- [waffle.Index], this is a list of indexes to be kept up to date
        whenever a Record is saved
    record_class -- subclass of waffle.Record, the main object record class; for instance, for a 'user' entity, one might have a User type that provides useful methods relating to operating on user properties.  Defaults to waffle.Record
    """
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
                sqlalchemy.Column('body', BinaryEncodedColumn(codec)),
                sqlalchemy.Column('updated', sqlalchemy.DATETIME(), default=datetime.datetime.now, onupdate=datetime.datetime.now),
                sqlalchemy.Column('created', sqlalchemy.DATETIME(), default=datetime.datetime.now)
            )

    def new(self):
        return self.record_class()

    def create(self):
        """Create all of the necessary database tables and indexes for this entity
        """
        for engine in self.engines:
            self.table.metadata.create_all(engine, checkfirst=True)

        for index in self.indices:
            index.create()

    def engine_for_uuid(self, uuid):
        """Bucket a UUID to a engine shard
        
        Override this if you want to control which object goes to which engine
        shard.  This implementation simply finds the modulus of the UUID's
        integer and the number of engines.  This should be reasonably
        distributed and easy to make consistent across platforms.
        """
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
            conn.execute(self.table.insert(), id=record.id, body=record.value)
        else:    
            conn.execute(self.table.update()
                    .where(record.c.id == record.id)
                    .values(body=record.value))
        trx.commit()
        conn.close()
        for index in self.indices:
            index.save(record)

    def lookup(self, record_ids):
        """Load records by ID

        Arguments:
        record_ids -- [UUID], a UUID for the record ID

        Returns:
        [records matching record_ids]
        """
        shards = defaultdict(set)
        num = len(self.engines)
        for record_id in record_ids:
            shards[self.engines[record_id.int % num]].add(record_id)    

        result = []
        for engine, sub_record_ids in shards.iteritems():
            conn = engine.connect()
            query = sql.select([self.table], self.table.c.id.in_(sub_record_ids))
            sub_result = conn.execute(query)
            result.extend(sub_result)
        
        records = []
        for r in result:
            rec = self.record_class(id=r.id, value=r.body, created=r.created, updated=r.updated)
            records.append(rec)
        return records

    @property 
    def c(self):
        return IndexClauseElement(self)

    def select(self, *clauses):
        """Run a select query
        
        Arguments
        *clauses -- SQLAlchemy index clauses

        Returns:
        [records matching clauses]
        """
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

class Index(object):
    """A persistent database index for index values 

    This is backed by a SQL Table with an compound index over all of the column values.

    Arguments:
    name -- str, the SQL name of the index
    columns -- [sqlalchemy.Column], a list of columns to be indexed
    shard -- an instance of IndexShardStrategy.  The strategy for picking shards for queries and index values.  For instance the ShardByPrimaryKey strategy groups all of the values for the same Record on one shard.
    mapper -- a function generator to map Record instances to an index value.  For example:
        def user_name_mapper(user_record):
            yield {'name': user_record.value['name']} 
    """
    def __init__(self, name, columns, mapper, shard):
        self.name = name
        self.metadata = sqlalchemy.MetaData()
        table_cols = list(columns) + [sqlalchemy.Column('id', UUIDColumn(), index=True)]
        self.table = sqlalchemy.Table(name, self.metadata, *table_cols)
        self.c = self.table.c 
        self.index = sqlalchemy.Index(name + '_idx', *columns)
        self.mapper = mapper
        self.shard = shard

    def select(self, clause):
        """Select values from a shard based on a clause"""
        results = []
        for engine in self.shard.engines_for_clauses(clause):
            conn = engine.connect()
            results_ = conn.execute(sql.select([self.table], clause))
            results.extend(results_)
            conn.close()
        return results

    def create(self):
        """Create the index"""
        for engine in self.shard.engines:
            self.metadata.create_all(engine, checkfirst=True)

    def save(self, record):
        """Update index values for this record.

        This does the following:
        - map a record to an index value
        - delete old record index values
        - insert new record index values
        """
        engine_to_mapping = defaultdict(list)
        for mapping in self.mapper(record):
            engine = self.shard.engine_for_record_mapping(record, mapping)
            engine_to_mapping[engine].append(mapping)

        for e in self.shard.engines:
            conn = engine.connect()
            trx = conn.begin()
            conn.execute(self.table.delete().where(self.table.c.id == record.id))
            for mapping in engine_to_mapping[engine]:
                conn.execute(self.table.insert().values(id=record.id, **mapping))
            trx.commit()
            conn.close()

class Record(object):
    """The basic record/row datatype for an Entity
    
    Arguments / Instance Attributes:
    id -- a UUID
    updated -- datetime.datetime or None, the time this was last saved
    created -- datetime.datetime or None, the time this was first saved
    value -- object, the user controlled bag of properties / values.  This is mostly opaque to the waffle system except that is must be capable of being serialized by the respective entity's codec.   All property customization should be done using this value

    Subclassing:
    If you subclass this and intend for a property to be kept consistent it should be stored in .value and be supported by the entity's codec.

    For example: 

    class User(Record):
        def __init__(**kw): 
            kw.setdefault('value', {})
            super(User, self).__init__(**kw)

        def _set_name(self, new_name):
            self.value['name'] = new_name

        def _get_name(self):
            return self.value.get('name', '')

        name = property(_get_name, _set_name, doc='The name of this user'):
    """
    def __init__(self, id=None, updated=None, created=None, value=None): 
        self.id = id if id is not None else uuid.uuid4()
        self.updated = updated if updated is not None else datetime.datetime.now()
        self.created = created if created is not None else datetime.datetime.now()
        self.value = value

    def __repr__(self):
        return 'Record(id=%(id)r, updated=%(updated)r, created=%(created)r, value=%(value)r)' % vars(self)

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
        'Record',
        'Table',
        'Codec',
        'ShardByPrimaryKey',
        'IndexShardStrategy',
        'CodecPipeline',
        'JSONCodec',
        'PickleCodec',
        'Table',
        'Index',
        ]
