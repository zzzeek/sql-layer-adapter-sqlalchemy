from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad
from ..dialect.base import NestedResult as _NestedResult, nested as _nested
from . import strategy

class ORMNestedResult(_NestedResult):
    hashable = False

    def __init__(self, query):
        self.query = query

    def foundationdb_result_processor(self, gen_nested_context):
        super_process = super(ORMNestedResult, self).\
            foundationdb_result_processor(gen_nested_context)
        def process(value):
            cursor = super_process(value)
            return list(
                self.query.instances(cursor)
            )
        return process

class orm_nested(_nested):
    """Convert a :class:`sqlalchemy:sqlalchemy.orm.query.Query` into a 'nested subquery'.

    This is the ORM analogue to the :class:`.nested` construct.  E.g.::

        from sqlalchemy_foundationdb import orm

        sess = Session()

        n = orm.orm_nested(sess.query(Order.id, Order).filter(Customer.orders))

        q = sess.query(Customer, n).filter(Customer.id == 1)

        for customer, orders in q:
            print "customer:", customer.name
            print "orders:", orders


    .. seealso::

        :ref:`orm_explicit_nested`

    """

    def __init__(self, query):
        stmt = query.statement
        super(orm_nested, self).__init__(stmt)
        self.type = ORMNestedResult(query)


@loader_option()
def nestedload(loadopt, attr):
    """Indicate that the given attribute should be loaded using "nested"
    loading.

    This function is part of the :class:`sqlalchemy:sqlalchemy.orm.strategy_options.Load`
    interface and supports
    both method-chained and standalone operation.

    e.g.::

        for customer in sess.query(Customer).options(orm.nestedload(Customer.orders)):
            print "customer:", customer.name
            print "orders:", customer.orders

    .. seealso::

        :ref:`orm_nested_eager_loading`

        :func:`.nestedload_all`


    """

    return loadopt.set_relationship_strategy(attr, {"lazy": "nested"})

@nestedload._add_unbound_fn
def nestedload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.nestedload, keys, False, {})

@nestedload._add_unbound_all_fn
def nestedload_all(*keys):
    """Multiple-attribute form of :func:`.nestedload`.

    Note that the "all" style of loading is deprecated in SQLAlchemy.
    Previously, to specify loading for each of a chain of attributes,
    the "all" option could be used as::

        sess.query(Customer).options(nestedload_all("orders", "items"))

    the new chaining style uses :func:`.nestedload` alone, as in::

        sess.query(Customer).options(nestedload("orders").nestedload("items"))

    .. seealso::

        :ref:`orm_nested_eager_loading`

        :func:`.nestedload`

    """
    return _UnboundLoad._from_keys(_UnboundLoad.nestedload, keys, True, {})

nestedload = nestedload._unbound_fn
nestedload_all = nestedload_all._unbound_all_fn
