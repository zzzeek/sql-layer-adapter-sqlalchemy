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

Note: Eventually we will want to add ``--requirements sqlalchemy_foundationdb.requirements:Requirements``, but that file isn't complete enough for the main sqlalchemy tests yet

Currently a bunch of tests fail when run with everything else due to DROP TABLE issues, running them individually should fix the problem, e.g.:

.. code-block:: sh

    fdbsqlcli -c 'DROP SCHEMA test CASCADE;'
    py.test --dburi foundationdb+psycopg2://@localhost:15432/test 'test/dialect/test_suite.py' -k test_autoincrement_col

A bunch of other tests fail because the requirements are not setup properly

The following tests in test/dialect/test_suite.py also fail:

* test_get_pk_constraint
* test_get_pk_constraint_with_schema
* test_get_table_names
* test_get_table_names_fks
* test_get_tables_and_views
* test_get_unique_constraints
* test_get_unique_constraints_with_schema
* test_round_trip
* test_bound_limit
* test_bound_limit_offset
* test_bound_offset
* test_limit_offset_nobinds
* test_many_significant_digits
