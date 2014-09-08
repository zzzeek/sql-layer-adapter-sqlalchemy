Running the unit tests
----------------------

You can run the tests for the sqlalchemy adapter by running (note: this uses the default schema, so make sure that schema is empty)

.. code-block:: sh

    pip install pytest mock
    python py.test


Running the sqlalchemy tests
----------------------------

To run SQLAlchemy's global tests with the fdbsql adapter, run:

.. code-block:: sh

    pip install pytest mock sqlalchemy-foundationdb
    py.test --dburi foundationdb+psycopg2://@localhost:15432/test
