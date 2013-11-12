import importlib
import threading
from cqlengine.exceptions import ValidationError
from cqlengine.management import sync_table
from cqlengine.models import BaseModel, ModelMetaClass
from cqlengine.query import BatchQuery


class SessionManager(object):
    def get_session(self):
        """Return current session for this context."""
        raise NotImplementedError

    def set_session(self, session):
        """Make the given session the current session for this context."""
        raise NotImplementedError


class ThreadLocalSessionManager(SessionManager):
    def __init__(self):
        self.storage = threading.local()

    def get_session(self):
        return getattr(self.storage, 'session', None)

    def set_session(self, session):
        self.storage.session = session


SESSION_MANAGER = ThreadLocalSessionManager()


def set_session_manager(manager):
    global SESSION_MANAGER
    SESSION_MANAGER = manager


def clear():
    """Empty the current session"""
    # xxx what happens to the existing id-map objects?  this is dangerous.
    # (also, the dev is not expected to call this.)
    SESSION_MANAGER.set_session(None)


def save():
    "Write all pending changes from session to Cassandra."
    session = SESSION_MANAGER.get_session()
    if session is not None:
        session.save()


def get_session(create_if_missing=True):
    session = SESSION_MANAGER.get_session()
    if session is None:
        session = Session()
        SESSION_MANAGER.set_session(session)
    return session


class Session(object):
    """Identity map objects and support for implicit batch save."""
    def __init__(self):
        self.instances_by_class = {}
        #self.creates = set()
        #self.deletes = set()

    def save(self):
        updates = set()
        creates = set()
        for model_class, by_key in self.instances_by_class.iteritems():
            for key, instance in by_key.iteritems():
                if instance._created:
                    creates.add(instance)
                elif hasattr(instance, '_dirties'):
                    updates.add(instance)
        with BatchQuery() as batch:
            for create in creates:
                values = {n: getattr(create, n) for n in create.id_mapped_class._columns.keys()}
                create.id_mapped_class.batch(batch).create(**values)
            for update in updates:
                raise NotImplementedError
                update.batch(batch).update()
#            for delete in self.deletes:
#                raise NotImplementedError

# xxx need to confirm, but I think if I define this in module we'll get what
# we want.  Otherwise, require the classes to inherit from SessionModel.
# (the other option may be to ... monkeypatch the meta class at the top of
# the model?)  That seems gross... it would be nice to be able to ride alongside
# a cqlengine model... like... somehow call a command to set it up... we'll see
# because cqlengine does its setup during the call to __new__, not during an
# explicitly called init().  (btw - this might make relationships more difficult
# to specify.)
class SessionModelMetaClass(ModelMetaClass):

    def __new__(cls, name, bases, attrs):
        if name == 'SessionModel':
            return super(SessionModelMetaClass, cls).__new__(cls,
                                                             name,
                                                             bases,
                                                             attrs)
        # Take the result of the base class's __new__ and assign it to the
        # module using a prefixed underscore in the name.
        # Create a class that will forward requests, read results, and
        # maintain an identity map.
        new_name = '_' + name
        # Note: at this point attrs has only those actually declared in
        # the class declaration
        base = super(SessionModelMetaClass, cls).__new__(cls,
                                                         new_name,
                                                         bases,
                                                         attrs)
        # Note: at this point, attrs has had a bunch of things added by
        # cqlengine.models.ModelMetaClass
        module = importlib.import_module(cls.__module__)
        setattr(module, new_name, base)
        # xxx I think we need to populate the dict with the attrs of
        # idmapmodel?
        # Note: if session wanted its own descriptors for the columns, it
        # could copy them from before the call to super above, and then
        # include them in the dict (seen as empty here).  Instead we
        # override setattr and introspect the columns at runtime, which I'm
        # not sure is better or worse at the moment.
        stand_in = IdMapMetaClass(name, (IdMapModel,), {})
        stand_in.id_mapped_class = base
        return stand_in


# declare your models with this so that SessionModelMetaClass is the metaclass.
class SessionModel(BaseModel):
    __abstract__ = True
    __metaclass__ = SessionModelMetaClass

class IdMapMetaClass(type):
    def __call__(cls, key=None):
        """If instance is in the id-map, return it, else make and return it."""
        session = get_session()
        try:
            instance_by_key = session.instances_by_class[cls]
            try:
                return instance_by_key[key]
            except KeyError:
                pass
        except KeyError:
            instance_by_key = {}
            session.instances_by_class[cls] = instance_by_key
        instance = super(IdMapMetaClass, cls).__call__(key=key)
        instance_by_key[key] = instance
        return instance


class IdMapModel(object):

    __metaclass__ = IdMapMetaClass

    def __init__(self, key=None):
        mapped_class = self.id_mapped_class
        name, column = mapped_class._primary_keys.items()[0]
        if key is None:
            if column.default:
                if callable(column.default):
                    key = column.default()
                else:
                    key = column.default
            else:
                raise ValueError(u"Can't create {} without providing primary key {}".format(self.__class__.__name__, name))
        self.key = key
        self.promote(name, key)

    # Override 'create' so that it does not call the query, but does let the
    # session know to insert the object.  cqlengine create will be called
    # on session save().
    @classmethod
    def create(mapper_class, **kwargs):
        cls = mapper_class.id_mapped_class
        extra_columns = set(kwargs.keys()) - set(cls._columns.keys())
        if extra_columns:
            raise ValidationError("Incorrect columns passed: {}".format(extra_columns))

        # Here we do sessioned create.  the cqlengine
        # will just do a save immediately, we want to hold off the save
        # until session.save() and we want it to not save if the session
        # never saves (is replaced by a new session aka cleared)
        key_name = cls._primary_keys.keys()[0]
        try:
            key = kwargs[key_name]
        except KeyError:
            key = None

        instance = mapper_class(key)
        instance._created = True
        for name, column in cls._columns.items():
            try:
                value = kwargs[name]
            except KeyError:
                if column.default:
                    if callable(column.default):
                        value = column.default()
                    else:
                        value = column.default
                elif name in cls._primary_keys:
                    raise ValueError(u"Can't create {} without providing primary key {}".format(cls.__name__, name))
                else:
                    value = None
            # Promote means "set without marking attribute as dirty."
            # in the case of a create, all attributes are "dirty" and are
            # ignored."
            instance.promote(name, value)
        # Hmmm... we could check if the thing exists at the top (that sounds
        # right) or we could check on add... and then what?  only add the
        # attrs that we don't have?  or do we just know that in load?
        #get_session().creates.add(instance)
        return instance

    # TOdO: could I even use the same generative syntax ('object') that
    # cqlengine uses?!?  perhaps perhaps.
    @classmethod
    def load(mapper_class, query):
        # a cqlengine query that either returns an instance or list of
        # instances of this class.
        session = get_session()
        result = query()
        if not isinstance(result, iterable):
            result = [result]
        for instance in result:
            # If the instance already exists in the ID map, get it, if not, make it.

            # Hmmm... I wonder if we should have class and instance versions
            # such that the instace version need not include a redundant key
            # all all the redundant lookups thereof.  I use this here as it is
            # a more general case.
            key_name = mapper_class.id_mapped_class._primary_keys.keys()[0]
            key = instance[key_name]
            map_instance = session.find_or_make(mapper_class, key)
            # TODO: this thwarts not wanting to just have a {} on the instance
            # if we create them for everything we get back from the load.
            # Is there a fixed frozen empty dict we could just refer to?
            try:
                dirties = map_instance._dirties
            except AttributeError:
                dirties = EMPTY

            for name, value in instance.iteritems():
                if name != key_name and name not in dirties:
                    map_instance.promote(name, value)

    def promote(self, name, value):
        object.__setattr__(self, name, value)

    def __setattr__(self, name, value):
        # We do this here to prevent instantiation of N dicts on a large load.
        try:
            dirties = self._dirties
        except AttributeError:
            dirties = {}
            object.__setattr__(self, '_dirties', dirties)
        dirties[name] = value
        self.promote(name, value)

    @classmethod
    def sync_table(cls):
        sync_table(cls.id_mapped_class)

EMPTY = frozenset()