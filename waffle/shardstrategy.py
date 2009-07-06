"""Contains strategies for finding the correct engine for the data

 * ShardByPrimaryKey
"""

class IndexShardStrategy(object):
    """A strategy for sharding index values and queries
    """ 
    def __init__(self, engines):
        self.engines = engines
        self.num_engines = len(engines)

    def engines_for_record_mapping(self, record, mapping={}):
        """Get the engine for a record and mapping"""
        raise NotImplementedError("subclasses should implement engine_for_record_mapping")

    def engines_for_clauses(self, clauses):
        """Get all of the engines for a query"""
        return self.engines

class ShardByPrimaryKey(IndexShardStrategy):
    """An index shard strategy for grouping index values by record id"""
    def hashfunc(self, key):
        """Given a key/id return back the id of the element to hash to
        """
        return key % self.num_engines
    
    def engine_for_record_mapping(self, record, mapping={}):
        return self.engines[self.hashfunc(record.id.int)]


__all__ = [
    'ShardByPrimaryKey',
    'IndexShardStrategy',
    ]
