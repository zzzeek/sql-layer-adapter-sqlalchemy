from sqlalchemy.testing import fixtures
from sqlalchemy.schema import AddConstraint, CreateTable
from sqlalchemy import inspect
from sqlalchemy.testing.assertions import eq_
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
        eq_(order_fks[0]['options']['grouping'], True)


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
        eq_(item_fks[0]['options']['grouping'], True)

    def test_reflect_named_fk_grouping(self):
        self._test_reflect_named_fk_grouping()

    def test_reflect_named_fk_grouping_schema(self):
        self._test_reflect_named_fk_grouping("test_schema")
