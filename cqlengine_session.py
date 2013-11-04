
import threading
from cqlengine.exceptions import ValidationError
from cqlengine.models import BaseModel, ModelMetaClass
from cqlengine.query import BatchQuery

SESSION = threading.local()

def get_session(clear=False, create_if_missing=True):
    """Gets the global session, making a new one if one does not already exist.

    clear -- If True, make a new global session and return it
    create_if_missing -- If False, will return None if session does not exist.

    """
    session = None
    if not clear:
        try:
            session = SESSION.value
        except AttributeError:
            pass
    if not session and create_if_missing:
        session = Session()
        SESSION.value = session
    return session

def save(*args, **kwargs):
    "Write all pending changes from session to Cassandra."
    get_session().save(*args, **kwargs)

class Session(object):
    """Identity map objects and support for implicit batch save."""
    def __init__(self):
        self.instances_by_class = {}
        self.creates = set()
        self.deletes = set()

    def save(self):
        updates = set()
        for model_class, by_key in self.instances_by_class.iteritems():
            for key, instance in by_key.iteritems():
                for name, manager in instance._values.iteritems():
                    if manager.changed:
                        updates.add(instance)
                        break

        creates = self.creates - self.deletes
        updates = updates - self.creates - self.deletes
        with BatchQuery() as batch:
            for create in creates:
                values = {n: getattr(create, n) for n in create._columns.keys()}
                create.__class__.batch(batch).create(**values)
            for update in updates:
                update.batch(batch).update()
            for delete in self.deletes:
                raise NotImplementedError

class SessionModelMetaClass(ModelMetaClass):
    def __call__(cls, **values):
        # Look up this object in the identity map, if it exists return it, if
        # not, instantiate it, add it to the identity map, and return it.
        # xxx - session only works with single-primary key objects at present.
        key_name = cls._primary_keys.keys()[0]
        try:
            key = values[key_name]
        except KeyError:
            raise ValueError(u'Tried to instantiate {} without key {}'.format(
                    cls.__name__, key_name))
        session = get_session()
        try:
            instances_by_key = session.instances_by_class[cls.__name__]
        except KeyError:
            pass
        else:
            try:
                instance = instances_by_key[key]
            except KeyError:
                pass
            else:
                # For each value in values, set it on the object if it is
                # not already there.
                for column_name, value in values.items():
                    if column_name == key_name:
                        continue
                    # Get the value manager for the attribute.
                    manager = instance._values[column_name]
                    print manager
                    # If the value is unset, set the value.
                    # xxx how do we know if the value has been set?
                    setattr(instance, column_name, value)
                return instance

        # If we get here, that means we did not find an instance.
        instance = super(SessionModelMetaClass, cls).__call__(**values)
        try:
            instances_by_key = session.instances_by_class[cls.__name__]
        except KeyError:
            instances_by_key = {}
            session.instances_by_class[cls.__name__] = instances_by_key
        instances_by_key[key] = instance

        return instance


class SessionModel(BaseModel):
    __abstract__ = True
    __metaclass__ = SessionModelMetaClass

    # Override 'create' so that it does not call the query, but does let the
    # session know to insert the object.
    @classmethod
    def session_create(cls, **kwargs):
        extra_columns = set(kwargs.keys()) - set(cls._columns.keys())
        if extra_columns:
            raise ValidationError("Incorrect columns passed: {}".format(extra_columns))

        # Here we do sessioned create.  the cqlengine
        # will just do a save immediately, we want to hold off the save
        # until session.save() and we want it to not save if the session
        # never saves (is replaced by a new session aka cleared)
        create_values = {}
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
            create_values[name] = value
        instance = cls(**create_values)
        get_session().creates.add(instance)
        return instance

