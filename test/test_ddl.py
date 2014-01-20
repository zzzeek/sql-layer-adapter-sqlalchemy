from sqlalchemy.testing import fixtures
from sqlalchemy import Table, Column, MetaData, Integer, ForeignKeyConstraint, ForeignKey
from sqlalchemy.schema import AddConstraint, CreateTable
from sqlalchemy import schema
from sqlalchemy.testing.assertions import AssertsCompiledSQL, eq_

class DDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'foundationdb'

    def test_fk_constraint_plain(self):
        m1 = MetaData()
        t1 = Table('t1', m1, Column('x', Integer))
        t2 = Table('t2', m1, Column('x', Integer))

        self.assert_compile(
            AddConstraint(ForeignKeyConstraint([t1.c.x], [t2.c.x])),
            "ALTER TABLE t1 ADD FOREIGN KEY(x) REFERENCES t2 (x)"
        )

    def test_fk_constraint_grouping(self):
        m1 = MetaData()
        t1 = Table('t1', m1, Column('x', Integer))
        t2 = Table('t2', m1, Column('x', Integer))

        self.assert_compile(
            AddConstraint(ForeignKeyConstraint(
                    [t1.c.x], [t2.c.x], foundationdb_grouping=True)),
            "ALTER TABLE t1 ADD GROUPING FOREIGN KEY(x) REFERENCES t2 (x)"
        )

    def test_fk_constraint_local_plain(self):
        m1 = MetaData()
        t1 = Table('t1', m1, Column('x', Integer))
        t2 = Table('t2', m1, Column('x', Integer, ForeignKey('t1.x')))

        self.assert_compile(
            CreateTable(t2),
            "CREATE TABLE t2 (x INTEGER NULL, FOREIGN KEY(x) REFERENCES t1 (x))"
        )

    def test_fk_constraint_local_grouping(self):
        m1 = MetaData()
        t1 = Table('t1', m1, Column('x', Integer))
        t2 = Table('t2', m1, Column('x', Integer,
                            ForeignKey('t1.x', foundationdb_grouping=True)))

        self.assert_compile(
            CreateTable(t2),
            "CREATE TABLE t2 (x INTEGER NULL, GROUPING FOREIGN KEY(x) REFERENCES t1 (x))"
        )
