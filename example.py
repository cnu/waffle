import sqlalchemy
import waffle

engines = [
        sqlalchemy.create_engine('sqlite:///:memory:'), 
        sqlalchemy.create_engine('sqlite:///:memory:'), 
        #sqlalchemy.create_engine('sqlite:///s1.db', echo=True), 
        #sqlalchemy.create_engine('sqlite:///s2.db', echo=True),
]

def to_users(record):
    yield {'user_id': record.value['user_id']}

def to_days(record):
    yield {'day': record.value['day']}

events = waffle.Entity(
        'event', 
        engines=engines, 
        indices=[
            waffle.Index('event_user', 
                columns=[sqlalchemy.Column('user_id', waffle.UUIDColumn)], 
                partition=waffle.PartitionByPrimaryKey(engines), 
                mapper=to_users),
            waffle.Index('event_day', 
                columns=[
                    sqlalchemy.Column('day', sqlalchemy.String(length=16)),
                ], 
                partition=waffle.PartitionByPrimaryKey(engines),
                mapper=to_days),
            ])

events.create()

users = waffle.Entity('user', engines=engines)
users.create()

user = users.new()


event = events.new()
event.value = {
    'title': 'My Awesome event',
    'user_id': user.id,
    'day': 'monday',
    } 

# Save the event
events.save(event)

# Save the user
users.save(user)

# Load the event by user id:
result = events.select(events.indices.event_user.c.user_id == user.id)
print result[0]
