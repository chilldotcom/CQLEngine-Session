
class _SessionedClass(object):
    """Wraps a cqlengine class for use in a Session."""

    def __init__(self, session, wrapped_class, **kwargs):
        self.session = session
        self.wrapped_class = wrapped_class
#        for name, column in wrapped_class._primary_keys.iteritems():
        for name, column in wrapped_class._columns.iteritems():
            if name not in kwargs:
                if column.default:
                    if callable(column.default):
                        kwargs[name] = column.default()
                    else:
                        kwargs[name] = column.default
                elif name in wrapped_class._primary_keys:
                    raise ValueError(u"Can't create {} without providing primary key {}".format(wrapped_class, name))
                else:
                    kwargs[name] = None
        for k, v in kwargs.iteritems():
            object.__setattr__(self, k, v)
        self.kwargs = kwargs

class Session(object):

    def __init__(self):
        self.creates = set()

    def commit(self):
        """Write all pending changes to cqlengine."""
        for obj in self.creates:
            wrapped_obj = obj.wrapped_class.create(**{k: getattr(obj, k) for k in obj.wrapped_class._columns.keys()})
            wrapped_obj.save()

    def create(self, model_class, **kwargs):
        """Create instance of model_class and return an identity mapped handle.

        model_class -- Class that will be created when session is flushed.

        returns identity map handle to instance.

        """
        item = _SessionedClass(self, model_class, **kwargs)
        self.creates.add(item)
        return item

    def load(self, *args, **kwargs):
        """Fetch row from Cassandra and returned wrapped object."""
        inventory = kwargs.get('inventory')
        keys = kwargs.get('keys')
        if len(args) > 2:
            raise TypeError("load() takes at most 2 arguments ({} given)".format(len(args)))
        if len(args) == 2:
            model_class = args[0]
            keys = [args[1]]
        elif len(args) == 1:
            #if isinstance(args[0], _SessionedClass):
                #xxxx
            #else:
                # _handle = args[0]
                # model_class = _handle.model_class
                # keys = [_handle.key]
            raise NotImplementedError('need to be able to load with session.handle')
        # Get the object from cqlengine.
        # (note: I don't like that we are copying from cqlengine instead of
        # just fetching on our own.  I don't like that cqlengine gets all the
        # data yet we may want to only get some.
        # TODO: Do we check if it is already loaded, and thus spare a second
        # load?  Or do we assume that the second load is naively requested
        # and only fetch it if the data is not already in the identity map?
        # also, if data has already been set, and then we load, are we
        # sure to preserve the pre-existing data?
        if len(keys) > 1:
            raise NotImplementedError('can only handle single key primary keys')
        if len(model_class._primary_keys) > 1:
            raise NotImplementedError('can only handle single primary key objects')
        primary_key_name, _ = list(model_class._primary_keys.iteritems())[0]
        primary_key = keys[0]
        obj = model_class.objects(**{primary_key_name: primary_key}).get()
        wrapped_obj = _SessionedClass(self, model_class, **{k: getattr(obj, k) for k in model_class._columns.keys()})
        return wrapped_obj


class sessionmaker(object):
    """A configurable :class:`.Session` factory.

    The :class:`.sessionmaker` factory generates new
    :class:`.Session` objects when called, creating them given
    the configurational arguments established here.

    e.g.::

        # global scope
        Session = sessionmaker()

        # later, in a local scope, create and use a session:
        sess = Session()

    XXX the spirit is correct here but the details need to be changed. - MEC

    Any keyword arguments sent to the constructor itself will override the
    "configured" keywords::

        Session = sessionmaker()

        # bind an individual session to a connection
        sess = Session(bind=connection)

    The class also includes a method :meth:`.configure`, which can
    be used to specify additional keyword arguments to the factory, which
    will take effect for subsequent :class:`.Session` objects generated.
    This is usually used to associate one or more :class:`.Engine` objects
    with an existing :class:`.sessionmaker` factory before it is first
    used::

        # application starts
        Session = sessionmaker()

        # ... later
        engine = create_engine('sqlite:///foo.db')
        Session.configure(bind=engine)

        sess = Session()

    .. seealso:

        :ref:`session_getting` - introductory text on creating
        sessions using :class:`.sessionmaker`.

    """

    def __init__(self, class_=Session, **kw):
        """Construct a new :class:`.sessionmaker`.

        All arguments here except for ``class_`` correspond to arguments
        accepted by :class:`.Session` directly.  See the
        :meth:`.Session.__init__` docstring for more details on parameters.

        :param class_: class to use in order to create new :class:`.Session`
         objects.  Defaults to :class:`.Session`.
        :param \**kw: all other keyword arguments are passed to the constructor
         of newly created :class:`.Session` objects.

        """
        self.kw = kw
        # make our own subclass of the given class, so that
        # events can be associated with it specifically.
        self.class_ = type(class_.__name__, (class_,), {})

    def __call__(self, **local_kw):
        """Produce a new :class:`.Session` object using the configuration
        established in this :class:`.sessionmaker`.

        In Python, the ``__call__`` method is invoked on an object when
        it is "called" in the same way as a function::

            Session = sessionmaker()
            session = Session()  # invokes sessionmaker.__call__()

        """
        for k, v in self.kw.items():
            local_kw.setdefault(k, v)
        return self.class_(**local_kw)

    def configure(self, **new_kw):
        """(Re)configure the arguments for this sessionmaker.

        e.g.::

            Session = sessionmaker()

            Session.configure(bind=create_engine('sqlite://'))
        """
        self.kw.update(new_kw)

    def __repr__(self):
        return "%s(class_=%r%s)" % (
                    self.__class__.__name__,
                    self.class_.__name__,
                    ", ".join("%s=%r" % (k, v) for k, v in self.kw.items())
                )

