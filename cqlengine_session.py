import copy
import importlib
import threading
from cqlengine import columns
from cqlengine.exceptions import ValidationError
from cqlengine.management import sync_table
from cqlengine.models import BaseModel, ModelMetaClass
from cqlengine.query import BatchQuery, ModelQuerySet


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
        #self.deletes = set()

    def save(self):
        updates = set()
        creates = set()
        for model_class, by_key in self.instances_by_class.iteritems():
            for key, instance in by_key.iteritems():
                if hasattr(instance, '_created') and instance._created:
                    creates.add(instance)
                elif hasattr(instance, '_dirties'):
                    updates.add(instance)
        with BatchQuery() as batch:
            for create in creates:
                values = {n: getattr(create, n) for n in create.id_mapped_class._columns.keys()}
                create.id_mapped_class.batch(batch).create(**values)
            for update in updates:
                cqlengine_instance = update.id_mapped_class(**{update._key_name: update._key})
                for name, value in update._dirties.items():
                    setattr(cqlengine_instance, name, value)
                del update._dirties
                cqlengine_instance.batch(batch).update()
#            for delete in self.deletes:
#                raise NotImplementedError

class SessionModelMetaClass(ModelMetaClass):

    def __new__(cls, name, bases, attrs):
        if name == 'SessionModel':
            return super(SessionModelMetaClass, cls).__new__(cls,
                                                             name,
                                                             bases,
                                                             attrs)
        # Take the result of the base class's __new__ and assign it to the
        # module using a prefixed underscore in the name.
        new_name = '_' + name
        # Note: at this point attrs has only those actually declared in
        # the class declaration (and not in any parent class declaration)
        base = super(SessionModelMetaClass, cls).__new__(cls,
                                                         new_name,
                                                         bases,
                                                         attrs)
        # Note: at this point, attrs has had a bunch of things added by
        # cqlengine.models.ModelMetaClass
        module = importlib.import_module(cls.__module__)
        setattr(module, new_name, base)
        key_name = base._primary_keys.keys()[0]
        stand_in = IdMapMetaClass(name, (IdMapModel,), {
            '_key_name': key_name,
            'id_mapped_class': base
        })
        return stand_in


# declare your models with this so that SessionModelMetaClass is the metaclass.
class SessionModel(BaseModel):
    __abstract__ = True
    __metaclass__ = SessionModelMetaClass

class IdMapMetaClass(type):

    def __call__(cls, key):
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
        instance = super(IdMapMetaClass, cls).__call__(key)
        instance_by_key[key] = instance
        return instance


# this is copied from cqlengine, may need more modification..
class QuerySetDescriptor(object):
    def __get__(self, instance, session_class):
        return WrappedQuerySet(instance, session_class)


class IdMapModel(object):

    __metaclass__ = IdMapMetaClass

    objects = QuerySetDescriptor()

    def __init__(self, key):
        self.key = key
        self.promote(self._key_name, key)

    @classmethod
    def all(cls):
        return cls.objects.all()

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.objects.filter(*args, **kwargs)

    @classmethod
    def get(cls, *args, **kwargs):
        return cls.objects.get(*args, **kwargs)

    @classmethod
    def create(mapper_class, **kwargs):
        cls = mapper_class.id_mapped_class
        extra_columns = set(kwargs.keys()) - set(cls._columns.keys())
        if extra_columns:
            raise ValidationError("Incorrect columns passed: {}".format(extra_columns))

        key_name, column = cls._primary_keys.items()[0]
        try:
            key = kwargs[key_name]
        except KeyError:
            key = None
        if key is None:
            if column.default:
                if callable(column.default):
                    key = column.default()
                else:
                    key = column.default
            else:
                raise ValueError(u"Can't create {} without providing primary key {}".format(mapper_class.__name__, key_name))

        instance = mapper_class(key)
        instance._created = True
        for name, col in cls._columns.items():
            if name == key_name:
                continue
            try:
                value = kwargs[name]
            except KeyError:
                if col.default:
                    if callable(col.default):
                        value = col.default()
                    else:
                        value = col.default
                elif name in cls._primary_keys:
                    raise ValueError(u"Can't create {} without providing primary key {}".format(cls.__name__, name))
                # Container columns have non-None empty cases.
                value = None
            if isinstance(col, columns.BaseContainerColumn):
                if isinstance(col, columns.Set):
                    value = OwnedSet(instance, name, col.to_python(value))
                elif isinstance(col, columns.List):
                    value = OwnedList(instance, name, col.to_python(value))
                elif isinstance(col, columns.Map):
                    value = OwnedMap(instance, name, col.to_python(value))
            instance.promote(name, value)
        return instance

    def promote(self, name, value):
        """set without marking attribute as dirty."""
        object.__setattr__(self, name, value)

    def __setattr__(self, name, value):
        # We do this here to prevent instantiation of N dicts on a large load.
        col = self.id_mapped_class._columns.get(name, None)
        if col is None:
            return object.__setattr__(self, name, value)
        try:
            dirties = self._dirties
        except AttributeError:
            dirties = {}
            object.__setattr__(self, '_dirties', dirties)
        if isinstance(col, columns.BaseContainerColumn):
            if isinstance(col, columns.Set):
                value = OwnedSet(self, name, col.to_python(value))
            elif isinstance(col, columns.List):
                value = OwnedList(self, name, col.to_python(value))
            elif isinstance(col, columns.Map):
                value = OwnedMap(self, name, col.to_python(value))
        dirties[name] = value
        self.promote(name, value)

    def container_dirty(self, name, value):
        try:
            dirties = self._dirties
        except AttributeError:
            dirties = {}
            object.__setattr__(self, '_dirties', dirties)
        dirties[name] = value

    @classmethod
    def sync_table(cls):
        sync_table(cls.id_mapped_class)

    @classmethod
    def _construct_instance(cls, names, values):
        mapped_class = cls.id_mapped_class
        key_name, column = mapped_class._primary_keys.items()[0]
        key = None
        for name, value in zip(names, values):
            if name == key_name:
                key = column.to_python(value)
                break
        instance = cls(key=key)
        cleaned_values = {}
        for name, value in zip(names, values):
            if name == key_name:
                continue
            col = cls.id_mapped_class._columns[name]
            if isinstance(col, columns.BaseContainerColumn):
                if isinstance(col, columns.Set):
                    value = OwnedSet(instance, name, col.to_python(value))
                elif isinstance(col, columns.List):
                    value = OwnedList(instance, name, col.to_python(value))
                elif isinstance(col, columns.Map):
                    value = OwnedMap(instance, name, col.to_python(value))
            elif value is not None:
                value = col.to_python(value)
            cleaned_values[name] = value
        try:
            dirties = instance._dirties
        except AttributeError:
            dirties = EMPTY
        for name, value in cleaned_values.items():
            if name != key_name and name not in dirties:
                instance.promote(name, value)
        return instance

    @property
    def _key(self):
        return getattr(self, self._key_name)

class WrappedQuerySet(ModelQuerySet):
    def __init__(self, session_instance, session_class):
        self._session_instance = session_instance
        self._session_class = session_class

        if not isinstance(session_class.id_mapped_class.objects, ModelQuerySet):
            # If we run into something that is not a ModelQuerySet, let's
            # support it.  Because we need to copy the _result_constructor
            # method instead of providing a _construct_instance method
            # directly, this is necessary.  Perhaps it is something we'd
            # ask of cqlengine plugin in the future.
            raise NotImplementedError(u'only ModelQuerySet queries are supported')

        super(WrappedQuerySet, self).__init__(session_class.id_mapped_class)

    def _get_result_constructor(self, names):
        """ Returns a function that will be used to instantiate query results """
        if not self._values_list:
            return lambda values: self._session_class._construct_instance(names, values)
        else:
            columns = [self.model._columns[n] for n in names]
            if self._flat_values_list:
                return lambda values: columns[0].to_python(values[0])
            else:
                return lambda values: map(lambda (c, v): c.to_python(v), zip(columns, values))

    def __deepcopy__(self, memo):
        clone = self.__class__(self._session_instance, self._session_class)
        for k, v in self.__dict__.items():
            if k in ['_con', '_cur', '_result_cache', '_result_idx']: # don't clone these
                clone.__dict__[k] = None
            elif k == '_batch':
                # we need to keep the same batch instance across
                # all queryset clones, otherwise the batched queries
                # fly off into other batch instances which are never
                # executed, thx @dokai
                clone.__dict__[k] = self._batch
            else:
                clone.__dict__[k] = copy.deepcopy(v, memo)

        return clone


class OwnedSet(set):

    def __init__(self, owner, name, *args, **kwargs):
        self.owner = owner
        self.name = name
        super(OwnedSet, self).__init__(*args, **kwargs)

    def mark_dirty(self):
        self.owner.container_dirty(self.name, self)

    def add(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).add(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).remove(*args, **kwargs)

    def clear(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).clear(*args, **kwargs)

    def copy(self, *args, **kwargs):
        c = super(OwnedSet, self).copy(*args, **kwargs)
        if hasattr(self, '_dirty'):
            c._dirty = self._dirty
        return c

    def difference_update(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).difference_update(*args, **kwargs)

    def discard(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).discard(*args, **kwargs)

    def intersection_update(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).intersection_update(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).pop(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).remove(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).remove(*args, **kwargs)

    def symmetric_difference_update(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).symmetric_difference_update(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedSet, self).update(*args, **kwargs)

class OwnedList(list):

    def __init__(self, owner, name, *args, **kwargs):
        self.owner = owner
        self.name = name
        super(OwnedList, self).__init__(*args, **kwargs)

    def mark_dirty(self):
        self.owner.container_dirty(self.name, self)

    def append(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).append(*args, **kwargs)

    def extend(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).extend(*args, **kwargs)

    def insert(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).insert(*args, **kwargs)

    def pop(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).pop(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).remove(*args, **kwargs)

#    raise #xxx is extend an update?
 #   raise # are reverse and sort side-efffecters?

class OwnedMap(dict):

    def __init__(self, owner, name, *args, **kwargs):
        self.owner = owner
        self.name = name
        super(OwnedMap, self).__init__(*args, **kwargs)

    def mark_dirty(self):
        self.owner.container_dirty(self.name, self)

    def clear(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).clear(*args, **kwargs)

    def copy(self, *args, **kwargs):
        c = super(OwnedMap, self).copy(*args, **kwargs)
        if hasattr(self, '_dirty'):
            c._dirty = self._dirty
        return c

    def pop(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).pop(*args, **kwargs)

    def popitem(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).popitem(*args, **kwargs)

    def update(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).update(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).remove(*args, **kwargs)

#fromkeys
#setdefault

class Empty(object):
    def __contains__(self, item):
        return False

EMPTY = Empty()
