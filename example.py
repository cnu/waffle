import sqlalchemy
import waffle

engines = [
        sqlalchemy.create_engine('sqlite:///:memory:', echo=True),
        sqlalchemy.create_engine('sqlite:///:memory:', echo=True),
]

def to_users(record):
    yield {
            'user_id': record.value['user_id'],
    }

indices = [
        waffle.Index('event_user_id', 
            [sqlalchemy.Column('user_id', sqlalchemy.Binary(length=16))], engines, 
            shard_on=['user_id'], mapping_generator=to_users),
]

events = waffle.Entity('event', engines=engines, indices=indices)

events.create()
event = events.new()
event.value = {
        'title': 'My Awesome event',
        'user_id': 'red',
        } 
event.save()
