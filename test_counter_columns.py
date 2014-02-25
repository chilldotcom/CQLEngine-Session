import unittest
import uuid
from uuid import uuid4

from cqlengine_session import clear, save, SessionModel
from cqlengine import columns
from cqlengine.connection import setup
from cqlengine.management import create_keyspace, delete_keyspace
from cqlengine.models import ModelDefinitionException
from cqlengine.tests.base import BaseCassEngTestCase


class TestCounterModel(SessionModel):
    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    counter = columns.Counter()


class BaseTestCase(unittest.TestCase):

    model_classes = {}

    def setUp(self):
        keyspace = 'testkeyspace{}'.format(str(uuid.uuid1()).replace('-', ''))
        self.keyspace = keyspace
        clear()
        # Configure cqlengine's global connection pool.
        setup(['localhost:9160'], default_keyspace=keyspace)
        create_keyspace(keyspace)
        for class_name, creator in self.model_classes.items():
            setattr(self, class_name, creator)
            #sync_table(getattr(self, class_name))
            getattr(self, class_name).sync_table()

    def tearDown(self):
        delete_keyspace(self.keyspace)


class TestClassConstruction(BaseTestCase):

    model_classes = {}

    def test_defining_a_non_counter_column_fails(self):
        """ Tests that defining a non counter column field in a model with a counter column fails """
        with self.assertRaises(ModelDefinitionException):
            class model(SessionModel):
                partition = columns.UUID(primary_key=True, default=uuid4)
                counter = columns.Counter()
                text = columns.Text()


    def test_defining_a_primary_key_counter_column_fails(self):
        """ Tests that defining primary keys on counter columns fails """
        with self.assertRaises(TypeError):
            class model(SessionModel):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter(primary_ley=True)
                counter = columns.Counter()

        # force it
        with self.assertRaises(ModelDefinitionException):
            class model(SessionModel):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter()
                cluster.primary_key = True
                counter = columns.Counter()


class TestCounterColumn(BaseTestCase):

    model_classes = {'TestCounterModel': TestCounterModel}

    def test_updates(self):
        """ Tests that counter updates work as intended """
        instance = TestCounterModel.create()
        key = instance.partition
        print '-----------'
        instance.counter += 5
        print' ---calling save'
        save()
        print ' ---savedone'
        clear()

        actual = TestCounterModel.get(partition=key)
        assert actual.counter == 5

    def test_update_from_none(self):
        """ Tests that updating from None uses a create statement """
        instance = TestCounterModel.create()
        key = instance.partition
        instance.counter += 1
        save()
        clear()

        new = TestCounterModel.get(partition=key)
        assert new.counter == 1

    def test_new_instance_defaults_to_none(self):
        """ Tests that instantiating a new model instance will set the counter column to zero """
        instance = TestCounterModel.create()
        assert instance.counter == 0

    def test_blind_update(self):
        instance = TestCounterModel.create()
        key = instance.partition
        cluster = instance.cluster
        instance.blind_increment('counter', 3)
        save()
        clear()

        instance = TestCounterModel(key, cluster)
        instance.blind_increment('counter', 7)
        save()
        clear()

        new = TestCounterModel.get(partition=key)
        assert new.counter == 10

    def test_consecutive_updates(self):
        instance = TestCounterModel.create()
        key = instance.partition
        with self.assertRaises(AttributeError):
            instance.counter = 3
        instance.counter += 3
        assert instance.counter == 3
        save()
        assert instance.counter == 3

        instance.counter += 3
        assert instance.counter == 6
        instance.counter += 4
        assert instance.counter == 10
        save()
        assert instance.counter == 10

        instance.counter += 7
        assert instance.counter == 17
        save()
        assert instance.counter == 17
        clear()

        new = TestCounterModel.get(partition=key)
        assert new.counter == 17

        new.counter += new.counter
        assert new.counter == 34
        save()
        clear()

        new = TestCounterModel.get(partition=key)
        assert new.counter == 34

        x = new.counter
        x += 20
        assert new.counter == 34
