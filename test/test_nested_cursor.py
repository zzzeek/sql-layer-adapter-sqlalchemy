from sqlalchemy.testing import fixtures, config
from sqlalchemy.testing.assertions import eq_, is_, assert_raises_message
from .fixtures import cust_order_item, cust_order_data
from sqlalchemy import select, type_coerce, exc
from decimal import Decimal
from sqlalchemy_foundationdb import nested
from sqlalchemy.types import TypeDecorator, Integer

class _Fixture(object):
    @classmethod
    def define_tables(cls, metadata):
        cust_order_item(metadata)

    @classmethod
    def insert_data(cls):
        cust_order_data(cls)

    @classmethod
    def setup_classes(cls):
        class Customer(cls.Comparable):
            pass
        class Order(cls.Comparable):
            pass
        class Item(cls.Comparable):
            pass


class NestedTest(_Fixture, fixtures.TablesTest):
    __dialect__ = 'sqlalchemy_foundationdb'

    def test_nested_row(self):
        customer = self.tables.customer
        order = self.tables.order

        sub_stmt = nested(select([order]).where(order.c.customer_id
                                            == customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)

        r = config.db.execute(stmt)
        row = r.fetchone()
        sub_result = row['o']
        eq_(
            list(sub_result),
            [(101, 1, 'apple related'), (102, 1, 'apple related'),
                (103, 1, 'apple related')]
        )

    def test_double_nested_row(self):
        customer = self.tables.customer
        order = self.tables.order
        item = self.tables.item

        sub_sub_stmt = nested(select([item]).where(item.c.order_id ==
                                            order.c.id)).label('i')
        sub_stmt = nested(select([sub_sub_stmt]).where(order.c.customer_id ==
                                            customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)

        r = config.db.execute(stmt)
        row = r.fetchone()
        sub_result = row['o']
        sub_sub_result = sub_result.fetchone()['i']
        eq_(
            list(sub_sub_result),
            [(1001, 101, Decimal('9.99'), 1), (1002, 101, Decimal('19.99'), 2)]
        )
        sub_sub_result = sub_result.fetchone()['i']
        eq_(
            list(sub_sub_result),
            [(1003, 102, Decimal('9.99'), 1)]
        )

    def test_nested_type_trans(self):
        customer = self.tables.customer
        order = self.tables.order
        item = self.tables.item

        class SpecialType(TypeDecorator):
            impl = Integer

            def process_result_value(self, value, dialect):
                return str(value) + "_processed"

        sub_sub_stmt = nested(select([type_coerce(item.c.price, SpecialType)]).\
                                    where(item.c.order_id ==
                                            order.c.id)).label('i')
        sub_stmt = nested(select([sub_sub_stmt]).where(order.c.customer_id ==
                                            customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)
        r = config.db.execute(stmt)
        row = r.fetchone()
        sub_result = row['o']
        sub_sub_result = sub_result.fetchone()['i']
        eq_(
            list(sub_sub_result),
            [('9.99_processed',), ('19.99_processed',)]
        )

    def test_nested_text_w_option(self):
        # a statement that returns a multi-row, nested result.
        stmt = 'select customer.id, (select "order".id from "order" '\
                'where customer.id="order".customer_id) as n1 from '\
                'customer where customer.id=3'

        r = config.db.execution_options(foundationdb_nested=True).execute(stmt)
        row = r.fetchone()
        eq_(row['n1'].fetchall(), [(107,), (108,), (109,)])

    def test_nested_text_wo_option(self):
        # a statement that returns a multi-row, nested result.
        stmt = 'select customer.id, (select "order".id from "order" '\
                'where customer.id="order".customer_id) as n1 from '\
                'customer where customer.id=3'

        # without the option, it runs in "table" row mode and we
        # get an error.
        assert_raises_message(
            exc.DBAPIError,
            "Subquery returned more than one row",
            config.db.execute, stmt
        )
