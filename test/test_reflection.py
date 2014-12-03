import sqlalchemy as sa
from sqlalchemy.testing import fixtures
from sqlalchemy.schema import AddConstraint, CreateTable
from sqlalchemy import inspect
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.exclusions import SpecPredicate
from .fixtures import cust_order_item
from sqlalchemy import testing

class FDBReflectionTest(fixtures.TablesTest):
    run_inserts = run_deletes = None

    __dialect__ = 'foundationdb'

    @classmethod
    def define_tables(cls, metadata):
        if testing.requires.schemas.enabled:
            cust_order_item(metadata, "test_schema")
        cust_order_item(metadata)

    def _test_reflect_unnamed_fk_grouping(self, schema=None):
        meta = self.metadata
        insp = inspect(meta.bind)

        order_fks = insp.get_foreign_keys("order", schema=schema)
        eq_(len(order_fks), 1)
        eq_(order_fks[0]['referred_columns'], ['id'])
        eq_(order_fks[0]['referred_table'], 'customer')
        eq_(order_fks[0]['constrained_columns'], ['customer_id'])
        eq_(order_fks[0]['referred_schema'], schema)
        eq_(order_fks[0]['options']['foundationdb_grouping'], True)


    def test_reflect_unnamed_fk_grouping(self):
        self._test_reflect_unnamed_fk_grouping()

    def test_reflect_unnamed_fk_grouping_schema(self):
        self._test_reflect_unnamed_fk_grouping("test_schema")

    def _test_reflect_named_fk_grouping(self, schema=None):
        meta = self.metadata
        insp = inspect(meta.bind)

        item_fks = insp.get_foreign_keys("item", schema=schema)
        eq_(len(item_fks), 1)
        eq_(item_fks[0]['name'], 'item_order_fk')
        eq_(item_fks[0]['referred_columns'], ['id'])
        eq_(item_fks[0]['referred_table'], 'order')
        eq_(item_fks[0]['constrained_columns'], ['order_id'])
        eq_(item_fks[0]['referred_schema'], schema)
        eq_(item_fks[0]['options']['foundationdb_grouping'], True)

    @testing.fails_if(SpecPredicate('foundationdb', '<=', (1,9,5), 'not supported in 1.9.5'))
    def test_reflect_named_fk_grouping(self):
        self._test_reflect_named_fk_grouping()

    @testing.fails_if(SpecPredicate('foundationdb', '<=', (1,9,5), 'not supported in 1.9.5'))
    def test_reflect_named_fk_grouping_schema(self):
        self._test_reflect_named_fk_grouping("test_schema")

    def test_fk_options(self):
        """test that foreign key reflection includes options (on
        backends with {dialect}.get_foreign_keys() support)"""

        test_attrs = ('match', 'onupdate', 'ondelete')
        addresses_user_id_fkey = sa.ForeignKey(
            'users.id',
            name = 'addresses_user_id_fkey',
            match='SIMPLE',
            onupdate='CASCADE',
            ondelete='CASCADE',
        )

        meta = self.metadata
        sa.Table('users', meta,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.String(30)))
        sa.Table('addresses', meta,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('user_id', sa.Integer, addresses_user_id_fkey))
        meta.create_all()

        meta2 = sa.MetaData()
        meta2.reflect(testing.db)
        for fk in meta2.tables['addresses'].foreign_keys:
            ref = addresses_user_id_fkey
            for attr in test_attrs:
                eq_(getattr(fk, attr), getattr(ref, attr))
