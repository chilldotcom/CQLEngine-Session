"""
CQLEngine-Session

Your cqlengine model must inherit from cqlengine_session.SessionModel instead
of cqlengine.model.BaseModel

SessionModel will replace youl SessionModel declarations with classes of type
IdMapModel.  Your model module will get classes of type BaseModel with an
underscore prefixed to the name.

example:
class Foo(SessionModel):
    pass

results in Foo being a IdMapModel, and _Foo being a BaseModel.

Note that making blind handles requires you pass a key.
blind = Foo(key)

you can make changes and save them (without first needing to load the object.)
blind.title = u'new title'
save()

To create new object use create
foo = Foo.create()

"""

import copy
import importlib
import threading
from cqlengine import columns
from cqlengine.exceptions import ValidationError
from cqlengine.management import sync_table
from cqlengine.models import BaseModel, ColumnQueryEvaluator, ModelMetaClass
from cqlengine.query import BatchQuery, ModelQuerySet


class AttributeUnavailable(Exception):
    pass


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
        counter_updates = set()
        creates = set()
        counter_creates = set()
        for model_class, by_key in self.instances_by_class.iteritems():
            for key, instance in by_key.iteritems():
                if hasattr(instance, '_created') and instance._created:
                    if model_class.id_mapped_class._has_counter:
                        counter_creates.add(instance)
                    else:
                        creates.add(instance)
                elif hasattr(instance, '_dirties'):
                    if model_class.id_mapped_class._has_counter:
                        counter_updates.add(instance)
                    else:
                        updates.add(instance)
        with BatchQuery() as batch:
            for create in creates:
                key_names = create.id_mapped_class._columns.keys()
                arg = {name: getattr(create, name) for name in key_names}
                create.id_mapped_class.batch(batch).create(**arg)
            for update in updates:
                key_names = update.id_mapped_class._primary_keys.keys()
                arg = {name: getattr(update, name) for name in key_names}
                cqlengine_instance = update.id_mapped_class(**arg)
                for name, value in update._dirties.items():
                    setattr(cqlengine_instance, name, value)
                del update._dirties
                cqlengine_instance.batch(batch).update()
        # It would seem that batch does not work with counter?
        #with BatchQuery() as batch:
        for create in counter_creates:
            primary_key_names = create.id_mapped_class._primary_keys.keys()
            arg = {name: getattr(create, name) for name in primary_key_names}
            instance = create.id_mapped_class.create(**arg)
            for name, col in create.id_mapped_class._columns.items():
                if isinstance(col, columns.Counter):
                    val = getattr(create, name)
                    setattr(instance, name, val)
            instance.update()
        for update in counter_updates:
            primary_key_names = update.id_mapped_class._primary_keys.keys()
            arg = {name: getattr(update, name) for name in primary_key_names}
            cqlengine_instance = update.id_mapped_class(**arg)
            for name, value in update._dirties.items():
                setattr(cqlengine_instance, name, value)
            del update._dirties
            cqlengine_instance.update()
#            for delete in self.deletes:
#                raise NotImplementedError

class SessionModelMetaClass(ModelMetaClass):

    def __new__(cls, name, bases, attrs):
        if attrs.get('__abstract__'):
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

        these_attrs = {
            'id_mapped_class': base,
        }
        # Make descriptors for the columns so the instances will get/set
        # using a ColumnDescriptor instance.
        for col_name, col in base._columns.iteritems():
            these_attrs[col_name] = ColumnDescriptor(col)

        # Any attr we did not define ourself, copy from the cqlengine class.
        # (I suspect this is not quite right, perhaps we should ditch
        # the metaclass hackery and require you to use another way of
        # using a cqlengine declaration. - MEC)
        base_attrs = {}
        for klass in reversed(bases):
            for sub_klass in reversed(klass.mro()):
                base_attrs.update(sub_klass.__dict__)
        base_attrs.update(attrs)
        base_attrs.update(these_attrs)
        for key in IdMapModel.__dict__.keys():
            try:
                del base_attrs[key]
            except KeyError:
                pass
        # These are not available on SessionModel objects.
        for key in ['update', 'save']:
            try:
                del base_attrs[key]
            except KeyError:
                pass
        stand_in = IdMapMetaClass(name, (IdMapModel,), base_attrs)
        return stand_in


# declare your models with this so that SessionModelMetaClass is the metaclass.
class SessionModel(BaseModel):
    __abstract__ = True
    __metaclass__ = SessionModelMetaClass

class IdMapMetaClass(type):

    def __call__(cls, *key):
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
        instance = super(IdMapMetaClass, cls).__call__(*key)
        instance_by_key[key] = instance
        return instance


# this is copied from cqlengine, may need more modification..
class QuerySetDescriptor(object):
    def __get__(self, instance, session_class):
        return WrappedQuerySet(instance, session_class)


class IdMapModel(object):

    __metaclass__ = IdMapMetaClass

    objects = QuerySetDescriptor()

    def __init__(self, *key):
        self.key = key
        key_names = self.id_mapped_class._primary_keys.keys()
        for name, value in zip(key_names, key):
            self._promote(name, value)

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
    def create(cls, **kwargs):
        column_names = cls.id_mapped_class._columns.keys()
        extra_columns = set(kwargs.keys()) - set(column_names)
        if extra_columns:
            raise ValidationError(
                    "Incorrect columns passed: {}".format(extra_columns))

        primary_keys = cls.id_mapped_class._primary_keys
        uncleaned_values = {}
        for name, col in cls.id_mapped_class._columns.items():
            try:
                value = kwargs[name]
            except KeyError:
                if col.default:
                    if callable(col.default):
                        value = col.default()
                    else:
                        value = col.default
                elif isinstance(col, columns.Counter):
                    value = 0
                elif name in primary_keys:
                    raise ValueError(u"Can't create {} without providing primary key {}".format(cls.__name__, name))
                else:
                    # Container columns have non-None empty cases.
                    value = None
            uncleaned_values[name] = value

        key = []
        for name, col in primary_keys.items():
            key.append(col.to_python(uncleaned_values[name]))
        instance = cls(*key)
        instance._created = True
        for name, col in cls.id_mapped_class._columns.items():
            if name in primary_keys:
                continue
            value = uncleaned_values[name]
            if isinstance(col, columns.BaseContainerColumn):
                if isinstance(col, columns.Set):
                    value = OwnedSet(instance, name, col.to_python(value))
                elif isinstance(col, columns.List):
                    value = OwnedList(instance, name, col.to_python(value))
                elif isinstance(col, columns.Map):
                    value = OwnedMap(instance, name, col.to_python(value))
            elif value is not None:
                value = col.to_python(value)
            instance._promote(name, value)
        return instance

    def _promote(self, name, value):
        """set without marking attribute as dirty."""
        try:
            self._values[name] = value
        except AttributeError:
            self._values = {name: value}

    def _mark_dirty(self, name, value):
        """mark an attribute as dirty."""
        try:
            self._dirties[name] = value
        except AttributeError:
            self._dirties = {name: value}

    @classmethod
    def sync_table(cls):
        sync_table(cls.id_mapped_class)

    @classmethod
    def _construct_instance(cls, names, values):
        mapped_class = cls.id_mapped_class
        primary_keys = mapped_class._primary_keys
        values = dict(zip(names, values))
        key = []
        for name, col in primary_keys.items():
            key.append(col.to_python(values[name]))
        instance = cls(*key)
        cleaned_values = {}
        for name, value in values.items():
            if name in primary_keys:
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
            if name not in primary_keys and name not in dirties:
                instance._promote(name, value)
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
        self.owner._mark_dirty(self.name, self)

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
        self.owner._mark_dirty(self.name, self)

    def __setitem__(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).__setitem__(*args, **kwargs)

    def __setslice__(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).__setslice__(*args, **kwargs)

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

    def reverse(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).reverse(*args, **kwargs)

    def sort(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedList, self).sort(*args, **kwargs)


class OwnedMap(dict):

    def __init__(self, owner, name, *args, **kwargs):
        self.owner = owner
        self.name = name
        super(OwnedMap, self).__init__(*args, **kwargs)

    def mark_dirty(self):
        self.owner._mark_dirty(self.name, self)

    def __setitem__(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).__setitem__(*args, **kwargs)

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

    def setdefault(self, *args, **kwargs):
        self.mark_dirty()
        return super(OwnedMap, self).setdefault(*args, **kwargs)


class ColumnDescriptor(object):
    """
    Handles the reading and writing of column values to and from
    a model instance's value manager, as well as creating
    comparator queries
    """

    def __init__(self, column):
        """
        :param column:
        :type column: columns.Column
        :return:
        """
        self.column = column
        self.query_evaluator = ColumnQueryEvaluator(self.column)

    def __get__(self, instance, owner):
        """
        Returns either the value or column, depending
        on if an instance is provided or not

        :param instance: the model instance
        :type instance: Model
        """
        if instance:
            try:
                return instance._values[self.column.column_name]
            except (AttributeError, KeyError,):
                raise AttributeUnavailable
        else:
            return self.query_evaluator

    def __set__(self, instance, value):
        """
        Sets the value on an instance, raises an exception with classes
        TODO: use None instance to create update statements
        """
        if instance:
            col = self.column
            name = col.column_name
            if isinstance(col, columns.BaseContainerColumn):
                if isinstance(col, columns.Set):
                    value = OwnedSet(instance, name, col.to_python(value))
                elif isinstance(col, columns.List):
                    value = OwnedList(instance, name, col.to_python(value))
                elif isinstance(col, columns.Map):
                    value = OwnedMap(instance, name, col.to_python(value))
            instance._mark_dirty(name, value)
            instance._promote(name, value)
        else:
            raise AttributeError('cannot reassign column values')

    def __delete__(self, instance):
        """
        Sets the column value to None, if possible
        """
        if instance:
            if self.column.can_delete:
                raise NotImplementedError
            else:
                raise AttributeError('cannot delete {} columns'.format(self.column.column_name))


class Empty(object):
    def __contains__(self, item):
        return False

EMPTY = Empty()
