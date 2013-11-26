from datetime import date, datetime
import unittest
import uuid
from uuid import UUID

from cqlengine import columns
from cqlengine.connection import setup
from cqlengine.management import create_keyspace, delete_keyspace, sync_table
from cqlengine.models import Model
from cqlengine.query import DoesNotExist
from cqlengine_session import verify

def make_model(table_name, skip={}, different={}, index={'text_index': True}):
    def get_col(name, col):
        if name in skip:
            return None
        if name in different:
            return different[name]
        if name in index:
            return col(index=True)
        else:
            return col()

    class TestTable(Model):
        __table_name__ = table_name
        partition = columns.Text(primary_key=True)
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        title = get_col('title', columns.Text)
        text_index = get_col('text_index', columns.Text)
        done = columns.Boolean()
        pub_date = columns.DateTime()
        # One column type of each type that has its own validation method.
        bytes = columns.Bytes()
        ascii = columns.Ascii()
        text = columns.Text()
        integer = columns.Integer()
        bigint = columns.BigInt()
        varint = columns.VarInt()
        uuid2 = columns.UUID()
        float = columns.Float()
        decimal = columns.Decimal()
        date = columns.Date()
        datetime = columns.DateTime()
        timeuuid = columns.TimeUUID()
        boolean = columns.Boolean()
        setcol = columns.Set(columns.Integer)
        listcol = columns.List(columns.Integer)
        mapcol = columns.Map(columns.Text, columns.Integer)
    return TestTable


class VerifyTest(unittest.TestCase):

    def setUp(self):
        keyspace = 'testkeyspace{}'.format(str(uuid.uuid1()).replace('-', ''))
        self.keyspace = keyspace
        # Configure cqlengine's global connection pool.
        setup('localhost:9160', default_keyspace=keyspace)
        create_keyspace(keyspace)

    def tearDown(self):
        delete_keyspace(self.keyspace)

    def test_has_extra_field(self):
        Foo = make_model(table_name='Foo')
        sync_table(Foo)

        Foo2 = make_model(table_name='Foo', skip=set(['title']))
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.extra) == 1
        assert 'title' in result.extra

    def test_has_missing_field(self):
        Foo = make_model(table_name='Foo', skip=set(['title']))
        sync_table(Foo)

        Foo2 = make_model(table_name='Foo')
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.missing) == 1
        assert 'title' in result.missing

    def test_has_extra_index(self):
        Foo = make_model(table_name='Foo')
        sync_table(Foo)

        Foo2 = make_model(table_name='Foo', index={})
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert len(result.extra_indexes) == 1
        assert 'text_index' in result.extra_indexes

    def test_has_missing_index(self):
        Foo = make_model(table_name='Foo', index={})
        sync_table(Foo)

        Foo2 = make_model(table_name='Foo')
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.missing
        assert not result.extra
        assert not result.different
        assert not result.extra_indexes
        assert len(result.missing_indexes) == 1
        assert 'text_index' in result.missing_indexes

    #def test_has_different(self):
    #    Foo = make_model(table_name='Foo')
    #    sync_table(Foo)
    #
    #    Foo2 = make_model(table_name='Foo', different={'title': columns.Ascii()})
    #    results = verify(Foo2)
    #    assert len(results) == 1
    #    result = results[0]
    #
    #    assert not result.extra
    #    assert not result.missing
    #    assert len(result.different) == 1
    #    assert 'title' in result.different

    def test_has_two(self):
        Foo = make_model(table_name='Foo')
        Bar = make_model(table_name='Bar')
        sync_table(Foo)
        sync_table(Bar)

        results = verify(Foo, Bar)
        assert not results

    def has_extra_cf(self):
        Foo = make_model(table_name='Foo')
        Bar = make_model(table_name='Bar')
        sync_table(Foo)
        sync_table(Bar)

        results = verify(Foo)
        assert len(results) == 1
        result = results[0]
        assert result.model == u'Bar'
        assert result.is_extra

    def has_missing_cf(self):
        Foo = make_model(table_name='Foo')
        Bar = make_model(table_name='Bar')
        sync_table(Foo)
        #sync_table(Bar)

        results = verify(Foo, Bar)
        assert len(results) == 2
        if results[0].model == Foo:
            foo_result = results[0]
            bar_result = results[1]
        else:
            foo_result = results[1]
            bar_result = results[0]
        assert not foo_result.is_missing
        assert not foo_result.extra
        assert not foo_result.missing
        assert not foo_result.different

        assert bar_result.is_missing

