from datetime import datetime
import unittest
import uuid

from cqlengine import columns
from cqlengine.connection import setup
from cqlengine.management import create_keyspace, delete_keyspace, sync_table
from cqlengine.query import DoesNotExist
from cqlengine_session import clear, save, SessionModel


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


class BaseTestCase(unittest.TestCase):

    model_classes = {}

    def setUp(self):
        keyspace = 'testkeyspace{}'.format(str(uuid.uuid1()).replace('-', ''))
        self.keyspace = keyspace
        clear()
        # Configure cqlengine's global connection pool.
        setup('localhost:9160', default_keyspace=keyspace)
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
        print '----------------------------------------'
        print 'todo_key {}'.format(todo_key)
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
        todo.pub_date = datetime.now()

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
        todo.pub_date = datetime.now()
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
        todo.pub_date = datetime.now()
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


class NoDefaultTestCase(BaseTestCase):

    model_classes = {'Todo': make_no_default_todo_model}

    def test_basic_insert(self):
        self.assertRaises(ValueError, self.Todo.create, title='first', text='text1')
