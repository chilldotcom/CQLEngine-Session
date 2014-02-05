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
    def get_col(name, col, args=(), kwargs={}):
        if name in skip:
            return None
        if name in different:
            return different[name]
        if name in index:
            return col(*args, index=True, **kwargs)
        else:
            return col(*args, **kwargs)

    class TestTable(Model):
        __table_name__ = table_name
        partition = columns.Text(primary_key=True)
        uuida = columns.UUID(primary_key=True, default=uuid.uuid4)
        uuidb = get_col('uuidb', columns.UUID, kwargs={'primary_key':True, 'default':uuid.uuid4})
        uuidc = columns.UUID(primary_key=True, default=uuid.uuid4)
        uuidd = get_col('uuidd', columns.UUID, kwargs={'primary_key':True, 'partition_key':True, 'default':uuid.uuid4})
        uuide = get_col('uuide', columns.UUID, kwargs={'primary_key':True, 'partition_key':True, 'default':uuid.uuid4})
        uuidf = get_col('uuidf', columns.UUID, kwargs={'primary_key':True, 'partition_key':True, 'default':uuid.uuid4})
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

def make_counter_model(table_name, skip={}, different={}, index={'text_index': True}):
    def get_col(name, col, args=(), kwargs={}):
        if name in skip:
            return None
        if name in different:
            return different[name]
        if name in index:
            return col(*args, index=True, **kwargs)
        else:
            return col(*args, **kwargs)

    class TestCountTable(Model):
        __table_name__ = table_name
        uuid = columns.UUID(primary_key=True, default=uuid.uuid4)
        count1 = get_col('count1', columns.Counter)
        count2 = get_col('count2', columns.Counter)
        count3 = get_col('count3', columns.Counter)
    return TestCountTable


class VerifyTest(unittest.TestCase):

    def setUp(self):
        keyspace = 'testkeyspace{}'.format(str(uuid.uuid1()).replace('-', ''))
        self.keyspace = keyspace
        # Configure cqlengine's global connection pool.
        setup(['localhost:9160'], default_keyspace=keyspace)
        create_keyspace(keyspace)

    def tearDown(self):
        delete_keyspace(self.keyspace)

    def test_has_extra_field(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', skip=set(['title']))
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.extra) == 1
        assert 'title' in result.extra

    def test_has_missing_field(self):
        Foo = make_model(table_name='foo_bar', skip=set(['title']))
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar')
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.missing) == 1
        assert 'title' in result.missing

    def test_has_extra_primary_key_field(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', skip=set(['uuidb']))
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.extra) == 1
        assert 'uuidb' in result.extra

    def test_has_missing_primary_key_field(self):
        Foo = make_model(table_name='foo_bar', skip=set(['uuidb']))
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar')
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.missing) == 1
        assert 'uuidb' in result.missing

    def test_has_different_primary_key(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', different={'uuidb': columns.Ascii(primary_key=True, default=uuid.uuid4)})
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]
        assert not result.extra
        assert not result.missing
        assert len(result.different) == 1
        assert 'uuidb' in result.different

    def test_has_extra_partition_key_field(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', skip=set(['uuide']))
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.extra) == 1
        assert 'uuide' in result.extra

    def test_has_missing_partition_key_field(self):
        Foo = make_model(table_name='foo_bar', skip=set(['uuide']))
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar')
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.missing) == 1
        assert 'uuide' in result.missing

    def test_has_different_partition_key(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', different={'uuide': columns.Ascii(primary_key=True, partition_key=True, default=uuid.uuid4)})
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]
        assert not result.extra
        assert not result.missing
        assert len(result.different) == 1
        assert 'uuide' in result.different

    def test_has_extra_single_partition_key_field(self):
        Foo = make_model(table_name='foo_bar', skip={'uuidd', 'uuidf'})
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', skip={'uuidd', 'uuide', 'uuidf'})
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        # Note that 'partition' will be 'missing' too because Foo.partition
        # gets a default 'partition_key=True' when all the explicit
        # partition_keys are skipped.  So, the verify will report a partition
        # key 'partition'.
        # When uuide is not skipped, Foo2.partition is not a partition key.
        # When verifying Foo2 against Foo's schema partition will show up
        # as 'missing' because it is a missing partition_key (not a missing
        # column.)
        assert len(result.missing) == 1
        assert 'partition' in result.missing
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        assert len(result.extra) == 1
        assert 'uuide' in result.extra

    def test_has_missing_single_partition_key_field(self):
        Foo = make_model(table_name='foo_bar', skip={'uuidd', 'uuide', 'uuidf'})
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', skip={'uuidd', 'uuidf'})
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert not result.extra_indexes
        # Note that 'partition' will be 'missing' too because Foo.partition
        # gets a default 'partition_key=True' when all the explicit
        # partition_keys are skipped.  So, the verify will report a partition
        # key 'partition'.
        # When uuide is not skipped, Foo2.partition is not a partition key.
        # When verifying Foo2 against Foo's schema partition will show up
        # as 'missing' because it is a missing partition_key (not a missing
        # column.)
        assert len(result.missing) == 2
        assert 'uuide' in result.missing
        assert 'partition' in result.missing

    def test_has_different_single_partition_key(self):
        Foo = make_model(table_name='foo_bar', skip={'uuidd', 'uuidf'})
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar',
                          skip={'uuidd', 'uuidf'},
                          different={
                              'uuide': columns.Ascii(primary_key=True,
                                                     partition_key=True,
                                                     default=uuid.uuid4)})
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]
        assert not result.extra
        assert not result.missing
        assert len(result.different) == 1
        assert 'uuide' in result.different

    def test_has_ok_index(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        results = verify(Foo)
        print [result.report() for result in results]
        assert len(results) == 0


    def test_has_extra_index(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', index={})
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.missing
        assert not result.extra
        assert not result.different
        assert not result.missing_indexes
        assert len(result.extra_indexes) == 1
        assert 'text_index' in result.extra_indexes

    def test_has_missing_index(self):
        Foo = make_model(table_name='foo_bar', index={})
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar')
        results = verify(Foo2)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]

        assert not result.is_missing
        assert not result.missing
        assert not result.extra
        assert not result.different
        assert not result.extra_indexes
        assert len(result.missing_indexes) == 1
        assert 'text_index' in result.missing_indexes

    def test_has_different(self):
        Foo = make_model(table_name='foo_bar')
        sync_table(Foo)

        Foo2 = make_model(table_name='foo_bar', different={'title': columns.Ascii()})
        results = verify(Foo2)
        assert len(results) == 1
        result = results[0]

        assert not result.extra
        assert not result.missing
        assert len(result.different) == 1
        assert 'title' in result.different

    def test_has_two(self):
        Foo = make_model(table_name='foo_bar')
        Bar = make_model(table_name='Bar')
        sync_table(Foo)
        sync_table(Bar)

        results = verify(Foo, Bar)
        assert not results

    def test_has_extra_cf(self):
        Foo = make_model(table_name='foo_bar')
        Bar = make_model(table_name='baz_qux')
        sync_table(Foo)
        sync_table(Bar)

        results = verify(Foo)
        [result.report() for result in results]
        assert len(results) == 1
        result = results[0]
        assert result.model == u'baz_qux'
        assert result.is_extra

    def test_has_missing_cf(self):
        Foo = make_model(table_name='foo_bar')
        Bar = make_model(table_name='baz_qux')
        sync_table(Foo)

        results = verify(Foo, Bar)
        [result.report() for result in results]
        assert len(results) == 1
        bar_result = results[0]
        assert bar_result.is_missing

    def test_counter_verify(self):
        Foo = make_counter_model(table_name='foo_bar')
        sync_table(Foo)

        results = verify(Foo)
        assert len(results) == 0

