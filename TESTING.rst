Running the unit tests
----------------------

You can run the tests for the sqlalchemy adapter by running (note: this uses the default schema, so make sure that schema is empty)

.. code-block:: sh

    pip install pytest mock
    py.test


Running the sqlalchemy tests
----------------------------

To run SQLAlchemy's global tests with the fdbsql adapter, run:

.. code-block:: sh

    pip install pytest mock sqlalchemy-foundationdb
    py.test --dburi foundationdb+psycopg2://@localhost:15432/test

Currently a bunch of tests fail when run with everything else due to DROP TABLE issues, running them individually should fix the problem, e.g.:

.. code-block:: sh

    fdbsqlcli -c 'DROP SCHEMA test CASCADE;'
    py.test --dburi foundationdb+psycopg2://@localhost:15432/test 'test/dialect/test_suite.py' -k test_autoincrement_col
