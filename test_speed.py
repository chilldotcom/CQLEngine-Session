from datetime import date, datetime
import uuid
from uuid import UUID

from cqlengine import columns
from cqlengine_session import (add_call_after_save, \
                               AttributeUnavailable, \
                               clear, \
                               save, \
                               SessionModel)
from test_cqlengine_session import BaseTestCase

def groom_time(dtime):
    return datetime(*dtime.timetuple()[:6])

def now():
    return groom_time(datetime.now())

def make_foo_model():
    class Foo(SessionModel):
        user_id = columns.UUID(primary_key=True, default=uuid.uuid4)
        contact_id = columns.UUID(primary_key=True, partition_key=True, default=uuid.uuid4)
        created_on = columns.DateTime()
        contact_types = columns.Set(columns.Integer())
        record_id = columns.Integer()
        score = columns.Integer()

    return Foo

def make_bar_model():
    class Bar(SessionModel):
        user_id = columns.UUID(primary_key=True, partition_key=True, default=uuid.uuid4)
        delta_id = columns.TimeUUID(primary_key=True, clustering_order='desc', default=uuid.uuid1)
        delta_type = columns.Integer()
        from_user = columns.Boolean()
        contact_id = columns.UUID()
        contact_type = columns.Integer()
        record_id = columns.Integer()
        score = columns.Integer()

    return Bar


class SpeedTestCase(BaseTestCase):

    model_classes = {'Foo': make_foo_model,
                     'Bar': make_bar_model}

    # change 'disabled' to 'test' and a profile will be saved to 'cqesstats'
    def disabled_insert_speed(self):
        pub_date = datetime.now()
        for i in xrange(10000):
            foo = self.Foo.create(
                created_on=pub_date,
                #contact_types={1,2,3},
                record_id=i,
                score=i
            )
            bar = self.Bar.create(
                delta_type=i,
                from_user=True,
                contact_id=uuid.uuid4(),
                contact_type=i,
                record_id=i,
                score=i
            )

        import cProfile
        # save the session, and thus the object.
        cProfile.run('import cqlengine_session;cqlengine_session.save()', 'cqesstats')
        # run this on the command line to see the profile
        # echo 'import pstats;p = pstats.Stats("cqesstats");p.sort_stats("cumulative").print_stats(10)' | python


