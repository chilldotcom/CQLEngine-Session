from datetime import date, datetime
import unittest
import uuid
from uuid import UUID

from cqlengine import columns
from cqlengine.connection import setup
from cqlengine.exceptions import ValidationError
from cqlengine.management import create_keyspace, delete_keyspace
from cqlengine.query import DoesNotExist
from cqlengine_session import (add_call_after_save, \
                               AttributeUnavailable, \
                               clear, \
                               save, \
                               SessionModel)

def groom_time(dtime):
    return datetime(*dtime.timetuple()[:6])

def now():
    return groom_time(datetime.now())

def make_todo_model():
    class Todo(SessionModel):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime()

    return Todo

def make_no_default_todo_model():
    class Todo(SessionModel):
        uuid = columns.UUID(primary_key=True)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime()

    return Todo

def make_inherited_model():
    class IntermediateTodo(SessionModel):
        __abstract__ = True
        base_text = columns.Text()

    class Todo(IntermediateTodo):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime()

    return Todo

def make_multi_key_model():
    class Todo(SessionModel):
        partition = columns.UUID(primary_key=True, default=uuid.uuid4)
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime(primary_key=True, default=now)

    return Todo

def make_no_default_multi_key_model():
    class Todo(SessionModel):
        partition = columns.UUID(primary_key=True)
        uuid = columns.UUID(primary_key=True)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime(primary_key=True)

    return Todo

def make_counter_model():
    class TestCounterModel(SessionModel):
        partition = columns.UUID(primary_key=True, default=uuid.uuid4)
        cluster = columns.UUID(primary_key=True, default=uuid.uuid4)
        counter = columns.Counter()

    return TestCounterModel

def make_subclass_model():
    class FirstIntermediateTodo(SessionModel):
        __abstract__ = True
        base_text = columns.Text()
        this_is_a_class_var = 'classvar'

        @classmethod
        def this_is_a_class_method(cls):
            return 1

        overloaded = 'first'

    class SecondIntermediateTodo(FirstIntermediateTodo):
        __abstract__ = True
        base_text = columns.Text()
        overloaded = 'second'
        @classmethod
        def this_is_a_class_method(cls):
            return 2

    class Todo(SecondIntermediateTodo):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        text = columns.Text()
        done = columns.Boolean()
        pub_date = columns.DateTime()
        overloaded = 'todo'

    return Todo

def make_default_todo_model():
    class Todo(SessionModel):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        done = columns.Boolean()
        pub_date = columns.DateTime()
        # One column type of each type that has its own validation method.
        bytes = columns.Bytes(default=b'xyz')
        ascii = columns.Ascii(default='default ascii')
        text = columns.Text(default=u'default text')
        integer = columns.Integer(default=42)
        bigint = columns.BigInt(default=55)
        varint = columns.VarInt(default=22)
        uuid2 = columns.UUID(default=UUID('3ba7a823-52cd-11e3-8d17-c8e0eb16059b'))
        float = columns.Float(default=3.1459)
        decimal = columns.Decimal(default=12.345)
        date = columns.Date(default=now)
        datetime = columns.DateTime(default=now)
        timeuuid = columns.TimeUUID(default=UUID('d16f1c47-52fa-11e3-9057-c8e0eb16059b'))
        boolean = columns.Boolean(default=False)
        setcol = columns.Set(columns.Integer, default={1,2,3})
        listcol = columns.List(columns.Integer, default=[1,2,3])
        mapcol = columns.Map(columns.Text, columns.Integer, default={'a': 1, 'b': 2})
    return Todo

def make_required_todo_model():
    class Todo(SessionModel):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = columns.Text(max_length=60)
        done = columns.Boolean()
        pub_date = columns.DateTime()
        bytes = columns.Bytes(required=True)
        ascii = columns.Ascii(required=True)
        text = columns.Text(required=True)
        integer = columns.Integer(required=True)
        bigint = columns.BigInt(required=True)
        varint = columns.VarInt(required=True)
        uuid2 = columns.UUID(required=True)
        float = columns.Float(required=True)
        decimal = columns.Decimal(required=True)
        datetime = columns.DateTime(required=True)
        date = columns.Date(required=True)
        timeuuid = columns.TimeUUID(required=True)
        boolean = columns.Boolean(required=True)
        setcol = columns.Set(columns.Integer, required=True)
        listcol = columns.List(columns.Integer, required=True)
        mapcol = columns.Map(columns.Text, columns.Integer, required=True)

    return Todo

def make_instance_range_model():

    class InstanceRangeColumn(columns.Integer):
        """Column with a range of legal values defined per-instance."""

        def __init__(self, *args, **kwargs):
            """
            :param enum_set: EnumSet instance (required)
            """
            self.range = kwargs['range']
            del kwargs['range']
            super(InstanceRangeColumn, self).__init__(*args, **kwargs)

        def validate(self, value):
            if value is None:
                return
            if value not in self.range:
                raise ValidationError("{} not in range for {}".format(value, self.column_name))
            return value

        def to_python(self, value):
            return self.validate(value)

        def to_database(self, value):
            return self.validate(value)

    class Todo(SessionModel):
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        col123 = InstanceRangeColumn(range={1, 2, 3}, default=1)
        col456 = InstanceRangeColumn(range={4, 5, 6}, required=True)

    return Todo



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
            setattr(self, class_name, creator())
            #sync_table(getattr(self, class_name))
            getattr(self, class_name).sync_table()

    def tearDown(self):
        delete_keyspace(self.keyspace)

class BasicTestCase(BaseTestCase):

    model_classes = {'Todo': make_todo_model}

    def test_basic_insert(self):
        # create an object

        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        self.assertTrue(isinstance(todo_key, uuid.UUID))
        self.assertEqual(todo.title, 'first')
        self.assertEqual(todo.text, 'text1')
        self.assertEqual(todo.done, None)
        self.assertEqual(todo.pub_date, None)

        # Do a non-session execute to confirm it's not there.
        # (not sure if this is defined behavior, as it should look up the
        # object in the session to begin with?  I think in this case it is
        # getting the object back from storage and then linking it with the
        # object in the identity map, so, fixing this is TODO, as this
        # way of checking was from the old way of handling the objects.)
        raised = None
        try:
            self.Todo.id_mapped_class.objects(uuid=todo_key).get()
        except Exception, e:
            raised = e
            self.assertTrue(isinstance(e, DoesNotExist))
        else:
            self.assertTrue(False)

        # save the session, and thus the object.
        save()

        # Confirm some identity map functionality.
        self.assertIs(todo, todo)
        found = self.Todo.objects(uuid=todo_key).get()
        self.assertIs(found, todo)

        # Clear the session
        clear()

        found = self.Todo.objects(uuid=todo_key).get()
        self.assertFalse(found is todo)

        self.assertEqual(found.title, 'first')
        self.assertEqual(found.text, 'text1')
        # xxx boolean seems to not like None, and insists on False.
        #self.assertEqual(found.done, None)
        self.assertEqual(found.pub_date, None)

    def test_basic_update(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        old_todo = todo
        save()

        # Get a new session.
        clear()
        # Load the object into the session.
        todo = self.Todo.objects(uuid=todo_key).get()

        # confirm the session cleared.
        self.assertIsNot(todo, old_todo)

        # Set some values.
        todo.title = u'new title'
        todo.text = u'new text'
        todo.done = True
        todo.pub_date = now()

        # Confirm the local assignment.
        self.assertEqual(todo.uuid, todo_key)
        self.assertEqual(todo.title, u'new title')
        self.assertEqual(todo.text, u'new text')
        self.assertEqual(todo.done, True)

        save()

        # Confirm the object is readable after save.
        self.assertEqual(todo.uuid, todo_key)
        self.assertEqual(todo.title, u'new title')
        self.assertEqual(todo.text, u'new text')
        self.assertEqual(todo.done, True)

        old_todo = todo

        # Clear the session.
        clear()
        todo = self.Todo.objects(uuid=todo_key).get()
        # Confirm again the session is cleared.
        self.assertIsNot(todo, old_todo)
        self.assertEqual(todo.uuid, todo_key)
        self.assertEqual(todo.title, u'new title')
        self.assertEqual(todo.text, u'new text')
        self.assertEqual(todo.done, True)
        old_todo = todo

        # Test a blind update.
        clear()
        todo = self.Todo(todo_key)
        self.assertFalse(old_todo is todo)
        todo.title = u'new new title'
        self.assertEqual(todo.title, u'new new title')
        old_todo = todo
        save()

        clear()
        todo = self.Todo.objects(uuid=todo_key).get()
        self.assertFalse(old_todo is todo)
        self.assertEqual(todo.uuid, todo_key)
        self.assertEqual(todo.title, u'new new title')
        self.assertEqual(todo.text, u'new text')
        self.assertEqual(todo.done, True)

    def test_loaded_dirty_load(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        todo.title = u'new title'
        todo.text = u'new text'
        todo.done = True
        todo.pub_date = now()
        save()

        # Get a new session.
        clear()
        # Load the object into the session.
        todo = self.Todo.objects(uuid=todo_key).get()

        self.assertEqual(todo.uuid, todo_key)
        self.assertEqual(todo.title, u'new title')
        self.assertEqual(todo.text, u'new text')
        self.assertEqual(todo.done, True)

        # Change a value.
        todo.title = u'new new title'
        # And load again, the load should not clobber the local change.
        todo = self.Todo.objects(uuid=todo_key).get()
        self.assertEqual(todo.title, u'new new title')
        save()
        clear()
        todo = self.Todo.objects(uuid=todo_key).get()
        self.assertEqual(todo.title, u'new new title')

    def test_blind_dirty_load(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        todo.title = u'new title'
        todo.text = u'new text'
        todo.done = True
        todo.pub_date = now()
        save()

        # Get a new session.
        clear()
        # Get a blind handle to the object.
        todo = self.Todo(todo_key)
        # Change a value.
        todo.title = u'new new title'
        # Load. the load should not clobber the local change.
        load_todo = self.Todo.objects(uuid=todo_key).get()
        self.assertTrue(todo is load_todo)
        self.assertEqual(todo.title, u'new new title')
        save()
        clear()
        todo = self.Todo.objects(uuid=todo_key).get()
        self.assertEqual(todo.title, u'new new title')

    def test_multi_result_load(self):
        todo1 = self.Todo.create(title='first', text='text1')
        todo2 = self.Todo.create(title='second', text='text2')
        todo3 = self.Todo.create(title='third', text='text3')
        todo4 = self.Todo.create(title='fourth', text='text4')
        todo5 = self.Todo.create(title='fifth', text='text5')
        save()

        results = self.Todo.all()
        self.assertEqual(5, len(results))
        results = set(results)
        self.assertIn(todo1, results)
        self.assertIn(todo2, results)
        self.assertIn(todo3, results)
        self.assertIn(todo4, results)
        self.assertIn(todo5, results)

        todo1_key = todo1.uuid
        todo2_key = todo2.uuid
        todo3_key = todo3.uuid
        todo4_key = todo4.uuid
        todo5_key = todo5.uuid
        clear()

        results = self.Todo.all()
        self.assertEqual(5, len(results))
        keys = set([t.uuid for t in results])
        self.assertIn(todo1_key, keys)
        self.assertIn(todo2_key, keys)
        self.assertIn(todo3_key, keys)
        self.assertIn(todo4_key, keys)
        self.assertIn(todo5_key, keys)

    def test_missing_attributes(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        todo.title = u'title'
        todo.text = u'text'
        todo.done = True
        todo.pub_date = now()
        save()

        # Get a new session.
        clear()

        # Get a blind handle to the object.
        todo = self.Todo(todo_key)
        self.assertRaises(AttributeUnavailable, getattr, todo, 'title')
        # Load the data to this object.
        todo.get()
        self.assertEqual(todo.title, u'title')

    def test_blind_set_to_none(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        todo.title = u'title'
        todo.text = u'text'
        todo.done = True
        todo.pub_date = now()
        save()

        # Get a new session.
        clear()

        # Get a blind handle to the object.
        todo = self.Todo(todo_key)
        todo.title = None
        save()
        clear()

        todo = self.Todo.objects(uuid=todo_key).get()
        assert todo.title == None

    def test_multiple_saves(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid
        self.assertTrue(isinstance(todo_key, uuid.UUID))
        self.assertEqual(todo.title, 'first')
        self.assertEqual(todo.text, 'text1')
        self.assertEqual(todo.done, None)
        self.assertEqual(todo.pub_date, None)

        # Do a non-session execute to confirm it's not there.
        # (not sure if this is defined behavior, as it should look up the
        # object in the session to begin with?  I think in this case it is
        # getting the object back from storage and then linking it with the
        # object in the identity map, so, fixing this is TODO, as this
        # way of checking was from the old way of handling the objects.)
        raised = None
        try:
            self.Todo.id_mapped_class.objects(uuid=todo_key).get()
        except Exception, e:
            raised = e
            self.assertTrue(isinstance(e, DoesNotExist))
        else:
            self.assertTrue(False)

        # save the session, and thus the object.
        save()

        # confirm in cassandra (outside of cqe-session)
        check = self.Todo.id_mapped_class.objects(uuid=todo_key).get()
        self.assertIsNotNone(check)

        # delete outside of cqe-session.
        check.delete()

        # re-check, make sure it's gone
        raised = None
        try:
            self.Todo.id_mapped_class.objects(uuid=todo_key).get()
        except Exception, e:
            raised = e
            self.assertTrue(isinstance(e, DoesNotExist))
        else:
            self.assertTrue(False)

        # make another object and save that.  (note session has not been
        # cleared.)
        todo2 = self.Todo.create(title='second', text='text2')
        todo2_key = todo.uuid
        save()

        # re-check, make sure todo 1 is gone.
        raised = None
        try:
            self.Todo.id_mapped_class.objects(uuid=todo_key).get()
        except Exception, e:
            raised = e
            self.assertTrue(isinstance(e, DoesNotExist))
        else:
            self.assertTrue(False)

    def test_call_after_save(self):
        todo = self.Todo.create(title='first', text='text1')
        todo_key = todo.uuid

        was_called = []
        def this_was_called(*args, **kwargs):
            was_called.append((args, kwargs))

        def that_was_called(*args, **kwargs):
            was_called.append((args, kwargs))

        add_call_after_save(this_was_called, 'foo', 'bar', baz='abc', qux=123)
        add_call_after_save(that_was_called, 54321, 3.14)
        add_call_after_save(this_was_called, nub='nab')

        assert len(was_called) == 0

        save()

        assert len(was_called) == 3
        assert was_called[0] == (('foo', 'bar'), {'baz': 'abc', 'qux': 123})
        assert was_called[1] == ((54321, 3.14), {})
        assert was_called[2] == (tuple(), {'nub': 'nab'})

        was_called = []

        todo.text = 'text2'

        save()

        assert len(was_called) == 0

        add_call_after_save(that_was_called, 54321, 3.14)
        add_call_after_save(this_was_called, nub='nab3')
        add_call_after_save(this_was_called, 'foo3', 'bar3', baz='abc3', qux=123)

        todo.text = 'text3'

        save()

        assert len(was_called) == 3
        assert was_called[0] == ((54321, 3.14), {})
        assert was_called[1] == (tuple(), {'nub': 'nab3'})
        assert was_called[2] == (('foo3', 'bar3'), {'baz': 'abc3', 'qux': 123})

        was_called = []

        add_call_after_save(that_was_called, 54321, 3.14)
        add_call_after_save(this_was_called, 'foo4', 'bar4', baz='abc4', qux=123)
        add_call_after_save(this_was_called, nub='nab4')

        save()

        assert len(was_called) == 3
        assert was_called[0] == ((54321, 3.14), {})
        assert was_called[1] == (('foo4', 'bar4'), {'baz': 'abc4', 'qux': 123})
        assert was_called[2] == (tuple(), {'nub': 'nab4'})

        was_called = []

        save()

        assert len(was_called) == 0

    def test_single_insert(self):
        # create an object

        todo1 = self.Todo.create(title='first', text='text1')
        todo1_key = todo1.uuid
        todo2 = self.Todo.create(title='second', text='text2')
        todo2_key = todo2.uuid

        # This will save todo1 only.
        save(todo1)

        raised = None
        try:
            self.Todo.id_mapped_class.objects(uuid=todo2_key).get()
        except Exception, e:
            raised = e
            self.assertTrue(isinstance(e, DoesNotExist))
        else:
            self.assertTrue(False)

        assert self.Todo.id_mapped_class.objects(uuid=todo1_key).get()

        save()

        assert self.Todo.id_mapped_class.objects(uuid=todo2_key).get()

    def test_single_update(self):
        todo1 = self.Todo.create(title='first', text='text1')
        todo1_key = todo1.uuid
        todo2 = self.Todo.create(title='second', text='text2')
        todo2_key = todo2.uuid

        save()
        clear()

        todo1 = self.Todo.objects(uuid=todo1_key).get()
        todo2 = self.Todo.objects(uuid=todo2_key).get()

        todo1.text = 'changed1'
        todo2.text = 'changed2'

        save(todo2)

        assert self.Todo.id_mapped_class.objects(uuid=todo1_key).get().text == 'text1'
        assert self.Todo.id_mapped_class.objects(uuid=todo2_key).get().text == 'changed2'

        save()

        assert self.Todo.id_mapped_class.objects(uuid=todo1_key).get().text == 'changed1'
        assert self.Todo.id_mapped_class.objects(uuid=todo2_key).get().text == 'changed2'


class TestDefaultCase(BaseTestCase):

    model_classes = {'Todo': make_default_todo_model}

    def test_blind_update_default(self):
        """ tests blind update won't clobber existing values with a default """
        non_default_uuid = uuid.uuid4()
        non_default_timeuuid = uuid.uuid1()
        dtime = now()
        d = now().date()
        m0 = self.Todo.create(
            bytes=b'notdefault',
            ascii='not default',
            text=u'not default',
            integer=105,
            bigint=222,
            varint=202,
            uuid2=non_default_uuid,
            float=22.44,
            decimal=44.22,
            datetime=dtime,
            date=d,
            timeuuid=non_default_timeuuid,
            boolean=True,
            setcol={3, 4},
            listcol=[5, 6],
            mapcol={'x': 15})
        key = m0.uuid
        save()
        clear()

        # blind update.
        m1 = self.Todo(key)
        m1.pub_date = now()
        save()
        clear()

        m2 = self.Todo(key).get()
        assert m2.bytes == b'notdefault'
        assert m2.ascii == 'not default'
        assert m2.text == u'not default'
        assert m2.integer == 105
        assert m2.bigint == 222
        assert m2.varint == 202
        assert m2.uuid2 == non_default_uuid
        assert m2.float == 22.44
        assert m2.decimal == 44.22
        assert m2.datetime == dtime
        assert m2.date == d
        assert m2.timeuuid == non_default_timeuuid
        assert m2.boolean == True
        assert m2.setcol == {3, 4}
        assert m2.listcol == [5, 6]
        assert m2.mapcol == {'x': 15}

        # non-blind update.
        m2.pub_date = now()
        save()
        clear()

        m3 = self.Todo(key).get()
        assert m3.bytes == b'notdefault'
        assert m3.ascii == 'not default'
        assert m3.text == u'not default'
        assert m3.integer == 105
        assert m3.bigint == 222
        assert m3.varint == 202
        assert m3.uuid2 == non_default_uuid
        assert m3.float == 22.44
        assert m3.decimal == 44.22
        assert m3.datetime == dtime
        assert m3.date == d
        assert m3.timeuuid == non_default_timeuuid
        assert m3.boolean == True
        assert m3.setcol == {3, 4}
        assert m3.listcol == [5, 6]
        assert m3.mapcol == {'x': 15}


class TestRequiredCase(BaseTestCase):

    model_classes = {'Todo': make_required_todo_model}

    def test_blind_update_required(self):
        """ tests blind update won't complain about required values """
        non_default_uuid = uuid.uuid4()
        non_default_timeuuid = uuid.uuid1()
        dtime = now()
        m0 = self.Todo.create()
        m0.bytes = b'required'
        m0.ascii = 'required ascii'
        m0.text = u'required text'
        m0.integer = 105
        m0.bigint = 222
        m0.varint = 202
        m0.uuid2 = non_default_uuid
        m0.float = 22.44
        m0.decimal = 44.22
        m0.datetime = dtime
        m0.date = dtime.date()
        m0.timeuuid = non_default_timeuuid
        m0.boolean = True
        m0.setcol = {1}
        m0.listcol = [1]
        m0.mapcol = {'a': 22}
        key = m0.uuid
        save()
        clear()

        # Do a blind update that does not include a required column.
        m1 = self.Todo(key)
        m1.pub_date = now()
        save()
        clear()

        m2 = self.Todo(key).get()
        assert m2.bytes == b'required'
        assert m2.ascii == 'required ascii'
        assert m2.text == u'required text'
        assert m2.integer == 105
        assert m2.bigint == 222
        assert m2.varint == 202
        assert m2.uuid2 == non_default_uuid
        assert m2.float == 22.44
        assert m2.decimal == 44.22
        assert m2.datetime == dtime
        assert m2.date == dtime.date()
        assert m2.timeuuid == non_default_timeuuid
        assert m2.boolean == True
        assert m2.setcol == {1}
        assert m2.listcol == [1]
        assert m2.mapcol == {'a': 22}


class NoDefaultTestCase(BaseTestCase):

    model_classes = {'Todo': make_no_default_todo_model}

    def test_basic_insert(self):

        self.assertRaises(ValueError, self.Todo.create, title='first', text='text1')

class NoDefaultTestCase(BaseTestCase):

    model_classes = {'Todo': make_no_default_todo_model}

    def test_basic_insert(self):
        with self.assertRaises(ValueError):
            self.Todo.create(title='first', text='text1')

        self.Todo.create(uuid=uuid.uuid4())

class NoDefaultMultiTestCase(BaseTestCase):

    model_classes = {'Todo': make_no_default_multi_key_model}

    def test_basic_insert(self):
        with self.assertRaises(ValueError):
            self.Todo.create(title='first', text='text1')

        self.Todo.create(partition=uuid.uuid4(),
                         uuid=uuid.uuid4(),
                         pub_date=now())


class InheritedTestCase(BaseTestCase):

    model_classes = {'Todo': make_inherited_model}

    def test_basic(self):
        todo = self.Todo.create()
        todo.title = u'parent title'
        todo.base_text = u'base text'
        save()
        clear()
        todo = self.Todo.get()
        assert todo.title == u'parent title'
        assert todo.base_text == u'base text'

class MultiKeyTestCase(BaseTestCase):

    model_classes = {'Todo': make_multi_key_model}

    def test_basic(self):
        todo = self.Todo.create()
        todo.title = u'multitest'
        partition = todo.partition
        cluster1 = todo.uuid
        cluster2 = todo.pub_date
        save()
        clear()

        todo = self.Todo.objects(partition=partition, uuid=cluster1, pub_date=cluster2).get()
        assert todo.title == u'multitest'

        print '-------calling create---------'
        new_cluster2 = groom_time(datetime(2013, 11, 15, 16, 12, 10))
        todo2 = self.Todo.create(partition=partition, uuid=cluster1, pub_date=new_cluster2)
        self.assertIsNot(todo2, todo)
        save()

        print '-------making new instance w same stuff'
        todo3 = self.Todo(partition, cluster1, new_cluster2)
        self.assertIs(todo2, todo3)

        print '-----calling get-----'
        todo4 = self.Todo.objects(partition=partition,
                                  uuid=cluster1,
                                  pub_date=new_cluster2).get()
        assert todo4.pub_date == new_cluster2
        self.assertIs(todo2, todo4)

class IntrospectionTestCase(BaseTestCase):

    model_classes = {'Todo': make_todo_model,
                     'MultiTodo': make_multi_key_model,
                     'Counter': make_counter_model}

    def test_type(self):
        from cqlengine_session import IdMapMetaClass
        assert isinstance(self.Todo, IdMapMetaClass)
        assert isinstance(self.MultiTodo, IdMapMetaClass)
        assert isinstance(self.Counter, IdMapMetaClass)

    def test_class_vars(self):
        assert self.Todo.uuid
        assert self.Todo.title
        assert self.Todo.text
        assert self.Todo.done
        assert self.Todo.pub_date
        assert not self.Todo._has_counter
        assert 1 == len(self.Todo._primary_keys)
        assert self.Todo._primary_keys['uuid'] == self.Todo._columns['uuid']

        assert self.MultiTodo.uuid
        assert self.MultiTodo.title
        assert self.MultiTodo.text
        assert self.MultiTodo.done
        assert self.MultiTodo.pub_date
        assert not self.MultiTodo._has_counter
        assert 3 == len(self.MultiTodo._primary_keys)
        assert [('partition', self.MultiTodo._columns['partition']),
                ('uuid', self.MultiTodo._columns['uuid']),
                ('pub_date', self.MultiTodo._columns['pub_date'])] == list(self.MultiTodo._primary_keys.iteritems())

        assert self.Counter.partition
        assert self.Counter.cluster
        assert self.Counter.counter
        assert self.Counter._has_counter
        assert 2 == len(self.Counter._primary_keys)
        assert [('partition', self.Counter._columns['partition']),
                ('cluster', self.Counter._columns['cluster'])] == list(self.Counter._primary_keys.iteritems())

class SubClassTestCase(BaseTestCase):

    model_classes = {'Todo': make_subclass_model}

    def test_class_vars(self):
        assert self.Todo.this_is_a_class_var == 'classvar'
        assert self.Todo.this_is_a_class_method() == 2
        todo = self.Todo.create()
        assert todo.this_is_a_class_var == 'classvar'
        assert todo.this_is_a_class_method() == 2

        assert todo.overloaded == 'todo'
        key = todo.uuid
        save()
        clear()
        todo = self.Todo(todo.uuid)
        todo.title = 'testtitle'
        save()
        clear()
        todo = self.Todo.objects(uuid=key).get()
        assert todo.title == 'testtitle'
        todo.title = 'testtitle2'
        save()


class InstanceValidationTestCase(BaseTestCase):

    model_classes = {'Todo': make_instance_range_model}

    def test_insert(self):
        todo = self.Todo.create(col456=5)
        todo_key = todo.uuid
        self.assertTrue(isinstance(todo_key, uuid.UUID))
        self.assertEqual(todo.col123, 1)
        self.assertEqual(todo.col456, 5)

        save()

        todo.col123 = 2

        save()
        clear()

        todo = self.Todo.get(uuid=todo_key)
        assert todo.col123 == 2
        assert todo.col456 == 5




