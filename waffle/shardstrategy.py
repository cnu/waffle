"""Contains strategies for finding the correct engine for the data

 * ShardByPrimaryKey
"""

class IndexShardStrategy(object):
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

class ShardByPrimaryKey(IndexShardStrategy):
    """An index shard strategy for grouping index values by record id"""
    def engine_for_record_mapping(self, record, mapping):
        return self.engines[record.id.int % len(self.engines)]


__all__ = [
    'ShardByPrimaryKey',
    'IndexShardStrategy',
    ]
