import unittest

import sqlalchemy
import waffle


class Functional1(unittest.TestCase):
    def test(self):
        engines = [sqlalchemy.create_engine('sqlite:///:memory:') for i in range(10)]

        def to_users(record):
            yield {'user_id': record.value['user_id']}

        def to_days(record):
            yield {'day': record.value['day']}

        event_user_idx = waffle.Index('event_user', 
                columns=[sqlalchemy.Column('user_id', waffle.UUIDColumn)], 
                shard=waffle.ShardByPrimaryKey(engines), 
                mapper=to_users)

        event_day_idx = waffle.Index('event_day', 
                columns=[sqlalchemy.Column('day', sqlalchemy.String(length=16))], 
                shard=waffle.ShardByPrimaryKey(engines),
                mapper=to_days)

        events = waffle.Entity('event', engines=engines, indices=[event_user_idx, event_day_idx], compress=True)

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
        result = events.select(event_user_idx.c.user_id == user.id)
        assert len(result) == 1
        assert result[0].value['title'] == 'My Awesome event'
        assert result[0].value['user_id'] == user.id
        assert result[0].value['day'] == 'monday'

if __name__ == '__main__':
    unittest.main()

