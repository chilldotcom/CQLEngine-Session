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
from datetime import date, datetime
import importlib
import json
import threading
from uuid import UUID

from cqlengine import columns
from cqlengine.connection import connection_manager, execute
from cqlengine.exceptions import ValidationError
from cqlengine.management import get_fields, sync_table
from cqlengine.models import BaseModel, ColumnQueryEvaluator, ModelMetaClass
from cqlengine.operators import EqualsOperator
from cqlengine.query import BatchQuery, ModelQuerySet
from cqlengine.statements import WhereClause, SelectStatement, DeleteStatement, UpdateStatement, AssignmentClause, InsertStatement, BaseCQLStatement, MapUpdateClause, MapDeleteClause, ListUpdateClause, SetUpdateClause, CounterUpdateClause


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


def save(*objects):
    "Write all pending changes from session to Cassandra."
    session = SESSION_MANAGER.get_session()
    if session is not None:
        session.save(*objects)


def get_session(create_if_missing=True):
    session = SESSION_MANAGER.get_session()
    if session is None:
        session = Session()
        SESSION_MANAGER.set_session(session)
    return session


def add_call_after_save(callable, *args, **kwargs):
    """Call callable with given args and kwargs after next save."""
    get_session().call_after_save.append((callable, args, kwargs,))


class Session(object):
    """Identity map objects and support for implicit batch save."""
    def __init__(self):
        self.instances_by_class = {}
        self.call_after_save = []
        #self.deletes = set()

    def save(self, *objects):
        """Flush all pending changes to Cassandra.

        objects -- if not None, only operate on this or these object(s)

        """
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
        if objects:
            updates = updates and objects
            counter_updates = counter_updates and objects
            creates = creates and objects
            counter_creates = counter_creates and objects

        with BatchQuery() as batch:
            for create in creates:
                # Note we skip a lot of cqlengine code and create the
                # insert statement directly.
                # (this is the non-optimized code that is replaced below)
                #key_names = create.id_mapped_class._columns.keys()
                #arg = {name: getattr(create, name) for name in key_names}
                #create.id_mapped_class.batch(batch).create(**arg)
                # (end non-optimized code)
                # (begin optimized)
                # note: it might save time to memoize column family name
                # note: cqlengine-session doesn't yet support 'ttl'
                insert = InsertStatement(create.id_mapped_class.column_family_name())#, ttl=self._ttl)
                for name, col in create.id_mapped_class._columns.items():
                    val = col.validate(getattr(create, name))
                    if col._val_is_null(val):
                        continue
                    insert.add_assignment_clause(AssignmentClause(
                        col.db_field_name,
                        col.to_database(val)))
                # skip query execution if it's empty
                # caused by pointless update queries
                if not insert.is_empty:
                    batch.add_query(insert)
                # (end optimized)
                del create._created
                try:
                    del create._dirties
                except AttributeError:
                    pass
            for update in updates:
                key_names = update._primary_keys.keys()
                arg = {name: getattr(update, name) for name in key_names}
                dirties = update._dirties
                update.id_mapped_class.objects(**arg).batch(batch).update(**dirties)
                del update._dirties
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
            del create._created
            try:
                del create._dirties
            except AttributeError:
                pass
            instance.update()
        for update in counter_updates:
            statement = UpdateStatement(update.id_mapped_class.column_family_name())#, ttl=self._ttl)
            for name, value in update._dirties.items():
                col = update.id_mapped_class._columns[name]
                clause = CounterUpdateClause(col.db_field_name, value, 0, column=col)
                statement.add_assignment_clause(clause)

            for name, col in update.id_mapped_class._primary_keys.items():
                statement.add_where_clause(WhereClause(
                    col.db_field_name,
                    EqualsOperator(),
                    col.to_database(getattr(update, name))
                ))
            execute(statement)
            del update._dirties
#            for delete in self.deletes:
#                raise NotImplementedError
        for callable, args, kwargs in self.call_after_save:
            callable(*args, **kwargs)
        self.call_after_save = []


class SessionModelMetaClass(ModelMetaClass):

    def __new__(cls, name, bases, attrs):
        if attrs.get('__abstract__'):
            return super(SessionModelMetaClass, cls).__new__(cls,
                                                             name,
                                                             bases,
                                                             attrs)
        if len(bases) > 1:
            raise TypeError('SessionModel does not allow multiple inheritance')
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

        # Copy attrs from the base class because this class won't actually
        # inherit from these base classes.
        base_attrs = {}
        copyable_bases = []
        for klass in bases[0].mro():
            if klass == SessionModel:
                break
            copyable_bases.append(klass)
        for klass in reversed(copyable_bases):
            base_attrs.update(klass.__dict__)
        base_attrs.update(attrs)
        base_attrs['id_mapped_class'] = base
        base_attrs['_promotable_column_names'] = [name for name, c in base_attrs['_columns'].iteritems() if not c.primary_key]
        # Make descriptors for the columns so the instances will get/set
        # using a ColumnDescriptor instance.
        for col_name, col in base._columns.iteritems():
            if isinstance(col, columns.Counter):
                base_attrs[col_name] = CounterColumnDescriptor(col)
            else:
                base_attrs[col_name] = ColumnDescriptor(col)
        return IdMapMetaClass(name, (IdMapModel,), base_attrs)


# declare your models with this so that SessionModelMetaClass is the metaclass.
class SessionModel(BaseModel):
    __abstract__ = True
    __metaclass__ = SessionModelMetaClass

class IdMapMetaClass(type):

#    def __new__(cls, name, bases, attrs):
#        return None
#        return type(name, bases, attrs)

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

    def promote(self, **kwargs):
        """Set kwargs on entity without marking as dirty

        Invalid column names in kwargs raises an exception

        Promoting the value of a key raises an exception

        """
        extra_columns = set(kwargs.keys()) - self._promotable_column_names
        if extra_columns:
            raise ValidationError("Incorrect columns passed: {}".format(extra_columns))

        for col_name, col_value in kwargs.items():
            self._promote(col_name, col_value)

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
            # Ignore results for columns returned that are not in the schema.
            # (They may be present as a result of migrating an existing db.)
            col = cls.id_mapped_class._columns.get(name)
            if col:
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

    def blind_increment(self, name, value):
        col = self.id_mapped_class._columns[name]
        if not isinstance(col, columns.Counter):
            raise ValueError(u'Can only blind increment Counter columns, %s is a %s' % (name, type(col)))
        # Increment the current value, if any.
        try:
            values = self._values
        except AttributeError:
            pass
        else:
            try:
                values[name] += value
            except KeyError:
                pass

        # Increment the dirty value, if any.
        try:
            dirties = self._dirties
        except AttributeError:
            self._dirties = {name: value}
        else:
            try:
                dirties[name] += value
            except KeyError:
                dirties[name] = value


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
                raise AttributeUnavailable(instance, self.column.column_name)
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


class WrappedResponse(int):
    # This is necessary so that set knows it is getting set as the result of
    # an __iadd__ call and not a regular assignment.
    # Doing this is necessary because a WrappedInt, as below, would be
    # incrementable and would side-effect the counter.
    pass


class WrappedInt(int):
    def __iadd__(self, value):
        return WrappedResponse(value)


class CounterColumnDescriptor(ColumnDescriptor):
    # This was made to get += to do the right thing for counters.
    # see http://stackoverflow.com/questions/11987949/how-to-implement-iadd-for-a-python-property
    def __get__(self, instance, owner):
        """
        Returns either the value or column, depending
        on if an instance is provided or not

        :param instance: the model instance
        :type instance: Model
        """
        if instance:
            try:
                existing_value = instance._values[self.column.column_name] or 0
                return WrappedInt(existing_value)
            except (AttributeError, KeyError,):
                raise AttributeUnavailable(instance, self.column.column_name)
        else:
            return self.query_evaluator

    def __set__(self, instance, value):
        """
        Sets the value on an instance, raises an exception with classes
        TODO: use None instance to create update statements
        """
        if instance:
            if isinstance(value, WrappedResponse):
                name = self.column.column_name
                value = int(value)
                # Increment the current value, if any.
                try:
                    values = instance._values
                except AttributeError:
                    instance._values = {name: value}
                else:
                    try:
                        values[name] += value
                    except KeyError:
                        values[name] = value

                # Increment the dirty value, if any.
                try:
                    dirties = instance._dirties
                except AttributeError:
                    instance._dirties = {name: value}
                else:
                    try:
                        dirties[name] += value
                    except KeyError:
                        dirties[name] = value
            else:
                raise AttributeError('cannot assign to counter, use +=')
        else:
            raise AttributeError('cannot reassign column values')

class Empty(object):
    def __contains__(self, item):
        return False

EMPTY = Empty()


class VerifyResult(object):

    def __init__(self, model, is_missing=False):
        self.model = model
        self.is_missing = is_missing
        self.is_extra = False
        self.missing = set()
        self.extra = set()
        self.different = set()
        self.missing_indexes = set()
        self.extra_indexes = set()

    def has_errors(self):
        return self.is_missing or \
                self.is_extra or \
                self.missing or \
                self.extra or \
                self.different or \
                self.missing_indexes or \
                self.extra_indexes

    def report(self):
        try:
            name = self.model.__name__
        except:
            name = self.model
        if self.is_missing:
            return '{} does not have a column family (expected "{}")'.format(
                    name,
                    self.model.column_family_name(include_keyspace=False))
        if self.is_extra:
            return 'found unexpected column family "{}"'.format(name)
        logs = []
        if self.missing:
            logs.append('{} columns missing: {}'.format(
                name, ', '.join(self.missing)))
        if self.extra:
            logs.append('{} extra columns: {}'.format(
                name, ', '.join(self.extra)
            ))
        if self.different:
            logs.append('{} different columns: {}'.format(
                name, ', '.join(self.different)))
        if self.missing_indexes:
            logs.append('{} indexes missing: {}'.format(
                name, ', '.join(self.missing_indexes)
            ))
        if self.extra_indexes:
            logs.append('{} extra indexes: {}'.format(
                name, ', '.join(self.extra_indexes)
            ))
        return '\n'.join(logs)


    def __repr__(self):
        return 'VerifyResult({})'.format(self.model.__name__)


def verify(*models, **kwargs):
    ignore_extra = kwargs.get('ignore_extra', {})
    results = {}
    by_keyspace = {}
    by_cf = {}
    results = {}
    for model in models:
        ks_name = model._get_keyspace()
        try:
            by_keyspace[ks_name].add(model)
        except KeyError:
            by_keyspace[ks_name] = set([model])
        cf_name = model.column_family_name(include_keyspace=False)
        by_cf[cf_name] = model
        results[model] = VerifyResult(model)

    for keyspace, models in by_keyspace.items():
        with connection_manager() as con:
            query_result = con.execute(
#                "SELECT columnfamily_name from system.schema_columnfamilies WHERE keyspace_name = :ks_name",
                "SELECT columnfamily_name, key_aliases, key_validator, column_aliases, comparator from system.schema_columnfamilies WHERE keyspace_name = :ks_name",
                {'ks_name': ks_name}
            )
        tables = {}
        for columnfamily_name, partition_keys, partition_key_types, primary_keys, primary_key_types in query_result.results:
            partition_keys = json.loads(partition_keys)
            if len(partition_keys) > 1:
                partition_key_types = partition_key_types[len('org.apache.cassandra.db.marshal.CompositeType('):-1].split(',')[:len(partition_keys)]
            else:
                partition_key_types = [partition_key_types]
            primary_keys = json.loads(primary_keys)
            primary_key_types = primary_key_types[len('org.apache.cassandra.db.marshal.CompositeType('):].split(',')[:len(primary_keys)]
            item = {
                'cf': columnfamily_name,
                'partition_keys': partition_keys,
                'partition_key_types': partition_key_types,
                'primary_keys': primary_keys,
                'primary_key_types': primary_key_types
            }
            tables[columnfamily_name] = item
        for model in models:
            cf_name = model.column_family_name(include_keyspace=False)
            db_field_names = {col.db_field_name: col for name, col in model._columns.items()}
            result = results[model]
            # Check that model's cf is in db's tables.
            if cf_name not in tables:
                result.is_missing = True
            else:
                table_info = tables[cf_name]
                fields = get_fields(model)
                fields = {field.name: field.type for field in fields}
                for name, field_type in fields.iteritems():
                    # If field is missing, that's an error.
                    if name not in db_field_names:
                        result.extra.add(name)
                    # If field is present, check the type.
                    else:
                        col = db_field_names[name]
                        if isinstance(col, columns.Map):
                            if not field_type.startswith('org.apache.cassandra.db.marshal.MapType'):
                                result.different.add(col.column_name)
                        elif isinstance(col, columns.List):
                            if not field_type.startswith('org.apache.cassandra.db.marshal.ListType'):
                                result.different.add(col.column_name)
                        elif isinstance(col, columns.Set):
                            if not field_type.startswith('org.apache.cassandra.db.marshal.SetType'):
                                result.different.add(col.column_name)
                        else:
                            local_metadata = _type_to_metadata(col.db_type)
                            if local_metadata != field_type:
                                result.different.add(col.column_name)
                for name, kind in zip(table_info['partition_keys'], table_info['partition_key_types']):
                    if name not in db_field_names:
                        result.extra.add(name)
                    else:
                        col = db_field_names[name]
                        local_metadata = _type_to_metadata(col.db_type)
                        if local_metadata != kind:
                            result.different.add(col.column_name)
                for name, kind in zip(table_info['primary_keys'], table_info['primary_key_types']):
                    if name not in db_field_names:
                        result.extra.add(name)
                    else:
                        col = db_field_names[name]
                        local_metadata = _type_to_metadata(col.db_type)
                        if col.clustering_order == 'desc':
                            local_metadata = u'org.apache.cassandra.db.marshal.ReversedType({})'.format(local_metadata)
                        if local_metadata != kind:
                            result.different.add(col.column_name)

                for name, col in db_field_names.items():
                    # Handle primary keys from table-level data.
                    if col.primary_key:
                        if col.partition_key:
                            if name not in table_info['partition_keys']:
                                result.missing.add(col.column_name)
                            else:
                                local_metadata = _type_to_metadata(col.db_type)
                                i = table_info['partition_keys'].index(name)
                                if local_metadata != table_info['partition_key_types'][i]:
                                    result.different.add(col.column_name)
                        else:
                            if name not in table_info['primary_keys']:
                                result.missing.add(col.column_name)
                            else:
                                local_metadata = _type_to_metadata(col.db_type)
                                if col.clustering_order == 'desc':
                                    local_metadata = u'org.apache.cassandra.db.marshal.ReversedType({})'.format(local_metadata)
                                i = table_info['primary_keys'].index(name)
                                if local_metadata != table_info['primary_key_types'][i]:
                                    result.different.add(col.column_name)

                    # Primary keys are not listed in fields.
                    if not col.primary_key and name not in fields:
                        result.missing.add(col.column_name)
        for cf in tables:
            if cf not in by_cf and cf not in ignore_extra:
                result = VerifyResult(cf)
                result.is_extra = True
                results[cf] = result
        model_indexes = {}
        for model in models:
            this_model_indexes = {col.db_field_name: col for name, col in model._columns.items() if col.index}
            if this_model_indexes:
                model_indexes[model.column_family_name(include_keyspace=False)] = this_model_indexes
        with connection_manager() as con:
            _, idx_names = con.execute(
                "SELECT index_name from system.\"IndexInfo\" WHERE table_name=:table_name",
                {'table_name': model._get_keyspace()}
            )
        cassandra_indexes = {}
        for (idx,) in idx_names:
            try:
                cf, index_name = idx.split('.')
                look_for = 'index_{}_'.format(cf)
                index_name = index_name[len(look_for):]
            except ValueError:
                cf = None
                index_name = None
            if cf:
                try:
                    cassandra_indexes[cf].add(index_name)
                except KeyError:
                    cassandra_indexes[cf] = set([index_name])
        for cf, index_names in cassandra_indexes.items():
            if cf not in model_indexes:
                if cf not in by_cf:
                    result = VerifyResult(cf)
                    result.is_extra = True
                    results[cf] = result
                else:
                    model = by_cf[cf]
                    result = results[model]
                    result.extra_indexes.add(index_name)
            else:
                this_model_indexes = model_indexes[cf]
                if index_name not in this_model_indexes:
                    model = by_cf[cf]
                    result = results[model]
                    result.extra_indexes.add(index_name)
        for cf, this_model_indexes in model_indexes.items():
            for index_name in this_model_indexes.keys():
                if cf not in cassandra_indexes or index_name not in cassandra_indexes[cf]:
                    model = by_cf[cf]
                    result = results[model]
                    result.missing_indexes.add(index_name)

    results = {model: result for model, result in results.items() if result.has_errors()}

    return results.values()


# Some functions to aid reading the cassandra definitions.
def _metadata_to_type(s):
    return {
        'org.apache.cassandra.db.marshal.UUIDType': UUID,
        'org.apache.cassandra.db.marshal.DoubleType': float,
        'org.apache.cassandra.db.marshal.UTF8Type': unicode,
        'org.apache.cassandra.db.marshal.BooleanType': bool,
        'org.apache.cassandra.db.marshal.Int32Type': int,
        'org.apache.cassandra.db.marshal.LongType': long,
        'org.apache.cassandra.db.marshal.DateType': date
    }.get(s, s)

def _type_to_metadata(s):
    return {
        'int': 'org.apache.cassandra.db.marshal.Int32Type',
        'text': 'org.apache.cassandra.db.marshal.UTF8Type',
        'uuid': 'org.apache.cassandra.db.marshal.UUIDType',
        UUID: 'org.apache.cassandra.db.marshal.UUIDType',
        float: 'org.apache.cassandra.db.marshal.DoubleType',
        'double': 'org.apache.cassandra.db.marshal.DoubleType',
        unicode: 'org.apache.cassandra.db.marshal.UTF8Type',
        'boolean': 'org.apache.cassandra.db.marshal.BooleanType',
        bool: 'org.apache.cassandra.db.marshal.BooleanType',
        int: 'org.apache.cassandra.db.marshal.Int32Type',
        long: 'org.apache.cassandra.db.marshal.LongType',
        'bigint': 'org.apache.cassandra.db.marshal.LongType',
        date: 'org.apache.cassandra.db.marshal.DateType',
        'decimal': 'org.apache.cassandra.db.marshal.DecimalType',
        'timestamp': 'org.apache.cassandra.db.marshal.TimestampType',
        'varint': 'org.apache.cassandra.db.marshal.IntegerType',
        'timeuuid': 'org.apache.cassandra.db.marshal.TimeUUIDType',
        'ascii': 'org.apache.cassandra.db.marshal.AsciiType',
        'blob': 'org.apache.cassandra.db.marshal.BytesType',
        'counter': 'org.apache.cassandra.db.marshal.CounterColumnType'
    }.get(s, s)

