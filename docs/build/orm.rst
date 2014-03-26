ORM Integration
===============

SQLAlchemy-FoundationDB includes ORM extensions, importable from the ``sqlalchemy_foundationdb.orm`` package.


.. _orm_nested_eager_loading:

Nested Eager Loading
--------------------

The :func:`.orm.nestedload` and :func:`.orm.nestedload_all` provide relationship eager loading
making usage of an embedded nested result.  These are used just like SQLAlchemy's own
:func:`~sqlalchemy:sqlalchemy.orm.joinedload` and :func:`~sqlalchemy:sqlalchemy.orm.subqueryload` functions::

    from sqlalchemy.orm import relationship, Session
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy_foundationdb import orm

    Base = declarative_base()

    class Customer(Base):
        __table__ = customer
        orders = relationship("Order")

    class Order(Base):
        __table__ = order


    sess = Session(engine)

    for customer in sess.query(Customer).options(orm.nestedload(Customer.orders)):
        print "customer:", customer.name
        print "orders:", customer.orders

.. _orm_explicit_nested:

Explicit Nesting
----------------

The :class:`.orm.orm_nested` construct acts just like the core :class:`.nested` construct,
except that it is ORM-aware and accepts a :class:`~sqlalchemy:sqlalchemy.orm.query.Query` object; it will invoke
:class:`~sqlalchemy:sqlalchemy.orm.query.Query` style loading,
nested into the tuples returned by :class:`~sqlalchemy:sqlalchemy.orm.query.Query`::

        from sqlalchemy_foundationdb import orm

        sess = Session()

        n = orm.orm_nested(sess.query(Order.id, Order).filter(Customer.orders))

        q = sess.query(Customer, n).filter(Customer.id == 1)

        for customer, orders in q:
            print "customer:", customer.name
            print "orders:", orders

Above, we're taking advantage of a new convenience feature in SQLAlchemy 0.8, which is that
we can pass the ``Customer.orders`` class-level attribute directly to
:meth:`~sqlalchemy:sqlalchemy.orm.query.Query.filter`
in order to generate a correlated WHERE clause.   Alternatively, we could just spell this out::

    query.filter(Customer.id==Order.customer_id)





