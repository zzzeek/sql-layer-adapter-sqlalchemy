
import re

from sqlalchemy import sql, exc, util
from sqlalchemy.engine import default, reflection, ResultProxy
from sqlalchemy.sql import compiler, expression, text
from sqlalchemy import types as sqltypes, schema as sa_schema
from foundationdb_sql.api import NESTED_CURSOR
from sqlalchemy.ext.compiler import compiles
import collections

from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, \
        CHAR, TEXT, FLOAT, NUMERIC, DATETIME, VARBINARY, \
        DATE, BOOLEAN, REAL, TIMESTAMP, DECIMAL, \
        TIME

RESERVED_WORDS = set(
    ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
    "asymmetric", "both", "case", "cast", "check", "collate", "column",
    "constraint", "create", "current_catalog", "current_date",
    "current_role", "current_time", "current_timestamp", "current_user",
    "default", "deferrable", "desc", "distinct", "do", "else", "end",
    "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
    "having", "in", "initially", "intersect", "into", "leading", "limit",
    "localtime", "localtimestamp", "new", "not", "null", "off", "offset",
    "old", "on", "only", "or", "order", "placing", "primary", "references",
    "returning", "select", "session_user", "some", "symmetric", "table",
    "then", "to", "trailing", "true", "union", "unique", "user", "using",
    "variadic", "when", "where", "window", "with", "authorization",
    "between", "binary", "cross", "current_schema", "freeze", "full",
    "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
    "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
    ])

_DECIMAL_TYPES = (1231, 1700)
_FLOAT_TYPES = (700, 701, 1021, 1022)
_INT_TYPES = (20, 21, 23, 26, 1005, 1007, 1016)



class DOUBLE(sqltypes.Float):
    __visit_name__ = 'DOUBLE'


class NestedResult(sqltypes.TypeEngine):
    """A SQLAlchemy type representing a 'nested result set' delivered as a
    column value.

    This datatype is not applied to the actual columns of a table, but is
    instead used internally for result sets from a SELECT statement,
    for those columns which correspond to a nested result.

    .. seealso::

        :class:`.nested`

    """
    def foundationdb_result_processor(self, gen_nested_context):
        def process(value):
            return ResultProxy(gen_nested_context(value))
        return process

class nested(expression.ScalarSelect):
    """Provide a 'nested' subquery.

    :class:`.nested` is a subclass of SQLAlchemy's :class:`sqlalchemy:sqlalchemy.sql.expression.ScalarSelect`
    class, which represents a SELECT statement that normally returns exactly
    one row and one column.  However, FoundationDB allows such subqueries
    to return full result sets.   :class:`.nested` is a SQLAlchemy integration
    point which specifies such a subquery in a Core SQL expression::

        from sqlalchemy import select
        from sqlalchemy_foundationdb import nested

        sub_stmt = nested([order]).where(order.c.customer_id
                                                == customer.c.id).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)

        result = conn.execute(stmt)

    .. seealso::

        :ref:`core_nested_select`

    """
    __visit_name__ = 'foundationdb_nested'

    def __init__(self, stmt):
        if isinstance(stmt, expression.ScalarSelect):
            stmt = stmt.element
        elif not isinstance(stmt, expression.SelectBase):
            stmt = expression.select(util.to_list(stmt))

        super(nested, self).__init__(stmt)
        self.type = NestedResult()



colspecs = {
}

ischema_names = {
    "BIGINT": BIGINT,
    "CHAR": CHAR,
    "DATETIME": DATETIME,
    "DOUBLE": DOUBLE,
    "INT": INTEGER,
    "DECIMAL": DECIMAL,
    "TEXT": TEXT,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "VARBINARY": VARBINARY,
    "VARCHAR": VARCHAR,

}

@compiles(nested)
def _visit_foundationdb_nested(nested, compiler, **kw):
    saved_result_map = compiler.result_map
    if hasattr(compiler, '_foundationdb_nested'):
        compiler.result_map = compiler._foundationdb_nested[nested.type] = {}
    try:
        kw['force_result_map'] = True
        return compiler.visit_grouping(nested, **kw)
    finally:
        compiler.result_map = saved_result_map

class FDBCompiler(compiler.SQLCompiler):

    @util.memoized_property
    def _foundationdb_nested(self):
        return {}

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text += " \n LIMIT " + self.process(sql.literal(select._limit))
        if select._offset is not None:
            text += " OFFSET " + self.process(sql.literal(select._offset))
            if select._limit is None:
                text += " ROWS"  # OFFSET n ROW[S]
        return text

    def visit_sequence(self, seq):
        return "nextval('%s')" % self.preparer.format_sequence(seq)

    def returning_clause(self, stmt, returning_cols):
        columns = [
                self._label_select_column(None, c, True, False,
                                    dict(include_table=False))
                for c in expression._select_iterables(returning_cols)
            ]

        return 'RETURNING ' + ', '.join(columns)


class FDBDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):

        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type)

        if column.nullable is not None:
            if not column.nullable:
                colspec += " NOT NULL"
            else:
                colspec += " NULL"

        if column is column.table._autoincrement_column:
            colspec += " GENERATED BY DEFAULT AS IDENTITY"
            # TODO: can do start with/increment by here
            # seq_col = column.table._autoincrement_column
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec


    def visit_foreign_key_constraint(self, constraint):
        preparer = self.dialect.identifier_preparer
        text = ""
        if constraint.name is not None:
            text += "CONSTRAINT %s " % \
                        preparer.format_constraint(constraint)
        remote_table = list(constraint._elements.values())[0].column.table

        text += "%sFOREIGN KEY(%s) REFERENCES %s (%s)" % (
            "GROUPING "
                if constraint.dialect_options['foundationdb']['grouping']
                else "",
            ', '.join(preparer.quote(f.parent.name)
                      for f in constraint._elements.values()),
            self.define_constraint_remote_table(
                            constraint, remote_table, preparer),
            ', '.join(preparer.quote(f.column.name)
                      for f in constraint._elements.values())
        )
        #text += self.define_constraint_match(constraint)
        #text += self.define_constraint_cascades(constraint)
        #text += self.define_constraint_deferrability(constraint)
        return text

    def visit_create_sequence(self, create):
        text = "CREATE SEQUENCE %s" % \
                self.preparer.format_sequence(create.element)
        if create.element.increment is not None:
            text += " INCREMENT BY %d" % create.element.increment
        if create.element.start is not None:
            text += " START WITH %d" % create.element.start
        else:
            # for some reason my sequences are defaulting to start
            # with min int
            text += " START WITH 1"
        return text

    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s RESTRICT" % \
                self.preparer.format_sequence(drop.element)



class FDBTypeCompiler(compiler.GenericTypeCompiler):
    def visit_DOUBLE(self, type_):
        return "DOUBLE"

class FDBIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set(range(0, 10)).union(["_", "$"])

class FDBInspector(reflection.Inspector):
    pass



class FDBExecutionContext(default.DefaultExecutionContext):
    def get_result_processor(self, type_, colname, coltype):
        if self.compiled and type_ in self.compiled._foundationdb_nested:
            class NestedContext(object):
                result_map = self.compiled._foundationdb_nested[type_]
                dialect = self.dialect
                root_connection = self.root_connection
                engine = self.engine
                _translate_colname = None
                get_result_processor = self.get_result_processor

                def __init__(self, value):
                    self.cursor = value

            return type_.foundationdb_result_processor(NestedContext)
        else:
            return type_._cached_result_processor(self.dialect, coltype)

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
                "select nextval('%s', '%s')" % (
                    seq.schema or self.dialect.default_schema_name,
                    self.dialect.identifier_preparer.format_sequence(seq)),
                type_)

    def _table_identity_sequence(self, table):
        if '_foundationdb_identity_sequence' not in table.info:
            schema = table.schema or self.dialect.default_schema_name
            value = self.connection.scalar(
                sql.text(
                    "select sequence_name from "
                    "information_schema.columns "
                    "where table_schema=:schema and "
                    "table_name=:tname"
                ).bindparams(schema=schema, tname=table.name),
            )
            table.info['_foundationdb_identity_sequence'] = (schema, value)
        return table.info['_foundationdb_identity_sequence']


    def get_insert_default(self, column):
        if column.primary_key and column is column.table._autoincrement_column:
            if column.server_default and column.server_default.has_argument:

                # pre-execute passive defaults on primary key columns
                return self._execute_scalar("select %s" %
                                    column.server_default.arg, column.type)

            elif (column.default is None or
                        (column.default.is_sequence and
                        column.default.optional)):

                # execute the sequence associated with an IDENTITY primary
                # key column. for non-primary-key SERIAL, the ID just
                # generates server side.

                schema, seq_name = self._table_identity_sequence(column.table)

                stmt = "select nextval('\"%s\".\"%s\"')" % (schema, seq_name)
                return self._execute_scalar(stmt, column.type)

        return super(FDBExecutionContext, self).get_insert_default(column)



class FDBDialect(default.DefaultDialect):
    name = 'foundationdb'
    supports_alter = False
    max_identifier_length = 63
    supports_sane_rowcount = True

    supports_native_enum = False
    supports_native_boolean = True

    supports_sequences = True
    sequences_optional = True
    preexecute_autoincrement_sequences = True
    postfetch_lastrowid = False
    implicit_returning = True

    supports_default_values = True
    supports_empty_insert = False
    default_paramstyle = 'pyformat'
    ischema_names = ischema_names
    colspecs = colspecs

    statement_compiler = FDBCompiler
    ddl_compiler = FDBDDLCompiler
    type_compiler = FDBTypeCompiler
    preparer = FDBIdentifierPreparer
    execution_ctx_cls = FDBExecutionContext
    inspector = FDBInspector
    isolation_level = None

    supports_empty_insert = False
    supports_default_values = False

    dbapi_type_map = {
        NESTED_CURSOR: NestedResult()
    }

    construct_arguments = [
        (sa_schema.ForeignKeyConstraint, {
            "grouping": False,
        })
    ]

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    def initialize(self, connection):
        super(FDBDialect, self).initialize(connection)

    def on_connect(self):
        return None

    def _get_default_schema_name(self, connection):
        return connection.scalar("select CURRENT_SCHEMA")

    def has_schema(self, connection, schema):
        raise NotImplementedError("has_schema")

    def has_table(self, connection, table_name, schema=None):
        schema = schema or self.default_schema_name

        cursor = connection.execute(
            sql.text(
            "select table_name from information_schema.tables "
            "where table_schema=:schema and table_name=:tname"
            ),
            {"tname": table_name, "schema": schema}
        )
        return bool(cursor.first())

    def has_sequence(self, connection, sequence_name, schema=None):
        schema = schema or self.default_schema_name
        cursor = connection.execute(
            sql.text(
                "SELECT sequence_name FROM information_schema.sequences "
                "WHERE sequence_name=:name AND sequence_schema=:schema"
            ).bindparams(name=sequence_name, schema=schema)
        )

        return bool(cursor.first())

    def _get_server_version_info(self, connection):
        ver = connection.scalar("select server_version from "
                    "information_schema.server_instance_summary")
        m = re.search('(\d+)\.(\d+)\.(\d+)', ver)
        if (m):
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        else:
            raise Exception("Invalid version returned from server: " + ver)

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        cursor = connection.execute(
            sql.text("select schema_name from information_schema.schemata")
            )
        return [row[0] for row in cursor.fetchall()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name

        cursor = connection.execute(
            sql.text(
            "select table_name from information_schema.tables "
            "where table_schema=:schema AND table_type='TABLE'"
            ),
            {"schema": schema}
        )
        return [row[0] for row in cursor.fetchall()]


    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name

        cursor = connection.execute(
            sql.text(
            "select table_name from information_schema.tables "
            "where table_schema=:schema AND table_type='VIEW'"
            ).bindparams(schema=schema)
        )
        return [row[0] for row in cursor.fetchall()]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        cursor = connection.execute(
            sql.text(
            "select view_definition from information_schema.views "
            "where table_schema=:schema AND table_name=:viewname"
            ).bindparams(schema=schema, viewname=view_name),
        )
        return cursor.scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        stmt = text(
                    "SELECT column_name, data_type, is_nullable, "
                    "character_maximum_length, "
                    "numeric_precision, numeric_scale, "
                    "column_default, "
                    "is_identity, identity_start, identity_increment "
                    "FROM information_schema.columns "
                    "WHERE table_schema=:schema AND table_name=:table "
                    "ORDER BY ordinal_position").bindparams(
                        schema=schema or self.default_schema_name,
                        table=table_name
                    )

        columns = []
        for cname, type_, nullable, length, precision, \
            scale, default, is_ident, ident_start, ident_increment in connection.execute(stmt):

            try:
                coltype = ischema_names[type_]
            except KeyError:
                util.warn("Did not recognize type '%s' of column '%s'" %
                      (type_, cname))
                coltype = sqltypes.NULLTYPE
            else:
                if issubclass(coltype, sqltypes.Float):
                    coltype = coltype()
                elif issubclass(coltype, sqltypes.Numeric):
                    coltype = coltype(precision, scale)
                elif issubclass(coltype, sqltypes.String):
                    coltype = coltype(length)
                    if default:
                        default = "'%s'" % default.replace("'", "''")
                else:
                    coltype = coltype()
            autoincrement = is_ident == 'YES'
            nullable = nullable == 'YES'

            column_info = dict(name=cname, type=coltype, nullable=nullable,
                           default=default, autoincrement=autoincrement)

            columns.append(column_info)
        return columns

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return self._get_uq_pk_constraints(connection, "UNIQUE", table_name,
                                                schema, **kw)

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pks = self._get_uq_pk_constraints(connection, "PRIMARY KEY", table_name,
                                                schema, **kw)
        if pks:
            return pks[0]
        else:
            return None

    def _get_uq_pk_constraints(self, connection, type_, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name

        stmt = text("SELECT tc.constraint_name "
                "FROM information_schema.table_constraints AS tc "
                "WHERE tc.table_schema=:schema AND tc.table_name=:table "
                "AND tc.constraint_type=:type"
            ).bindparams(schema=schema, table=table_name, type=type_)

        if type_ == 'PRIMARY KEY':
            col_collection = 'constrained_columns'
        elif type_ == 'UNIQUE':
            col_collection = 'column_names'
        else:
            assert False

        constraints = {}
        for const_name, in connection.execute(stmt):
            if (self._get_server_version_info(connection) <= (1,9,5)):
                cname = const_name.split('.')[1]
            else:
                cname = const_name
            constraints[const_name] = {'name': cname, col_collection: []}

        stmt = text("SELECT tc.constraint_name, kcu.column_name "
                "FROM information_schema.table_constraints AS tc "
                " JOIN information_schema.key_column_usage AS kcu ON "
                    "tc.constraint_name=kcu.constraint_name "
                    "AND tc.constraint_schema=kcu.constraint_schema "
                    "WHERE tc.table_schema=:schema AND tc.table_name=:table "
                    "AND tc.constraint_type=:type "
                    "ORDER BY kcu.ordinal_position"
                ).bindparams(schema=schema, table=table_name, type=type_)

        for const_name, colname in connection.execute(stmt):
            constraints[const_name][col_collection].append(colname)

        return list(constraints.values())

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name

        fks = {}
        for grouping in False, True:
            stmt = text("SELECT rc.constraint_name, rfnced.table_schema, "
                            "rfnced.table_name "
                    "FROM information_schema.table_constraints AS tc "
                    "JOIN information_schema.%s AS rc "
                        "ON tc.constraint_name=rc.constraint_name "
                        "AND tc.constraint_schema=rc.constraint_schema "
                    "JOIN information_schema.table_constraints AS rfnced ON "
                    "rc.unique_%sschema=rfnced.constraint_schema AND "
                    "rc.unique_constraint_name=rfnced.constraint_name "
                    "WHERE tc.table_schema=:schema AND tc.table_name=:table "
                    % (
                            "referential_constraints"
                                if not grouping else "grouping_constraints",
                            "constraint_"
                                if not grouping else "",
                        )
                ).bindparams(schema=schema, table=table_name)

            for conname, referred_schema, referred_table in connection.execute(stmt):
                fks[conname] = {
                    'name': conname,
                    'constrained_columns': [],
                    'referred_schema': referred_schema
                                    if referred_schema != self.default_schema_name
                                    else None,
                    'referred_table': referred_table,
                    'referred_columns': [],
                    'options': {
                        'grouping': grouping
                        #'onupdate': onupdate,
                        #'ondelete': ondelete,
                    }
                }

            stmt = text("SELECT rc.constraint_name, lcl.column_name as lcn, rmt.column_name as rcn"
                    " FROM information_schema.table_constraints AS tc "
                    " JOIN information_schema.%s AS rc "
                        "ON tc.constraint_name=rc.constraint_name "
                        "AND tc.constraint_schema=rc.constraint_schema "
                    " JOIN information_schema.key_column_usage AS lcl ON "
                        "rc.constraint_name=lcl.constraint_name "
                        "AND rc.constraint_schema=lcl.constraint_schema "
                    " JOIN information_schema.key_column_usage AS rmt ON "
                        "rc.unique_%sschema=rmt.constraint_schema AND "
                        "rc.unique_constraint_name=rmt.constraint_name AND "
                        "lcl.ordinal_position=rmt.ordinal_position "
                        "WHERE tc.table_schema=:schema AND tc.table_name=:table "
                        "ORDER BY lcl.ordinal_position" %
                        (
                            "referential_constraints"
                                if not grouping else "grouping_constraints",
                            "constraint_"
                                if not grouping else "",
                        )
                    ).bindparams(schema=schema, table=table_name)


            for cname, lclname, rmtname in connection.execute(stmt):
                fk = fks[cname]
                fk['constrained_columns'].append(lclname)
                fk['referred_columns'].append(rmtname)

        return list(fks.values())

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        schema = schema or self.default_schema_name

        # note: foundationdb doubles unique indexes as unique
        # constraints, the same way as Postgresql does.

        stmt = text("SELECT ix.index_name, ix.is_unique "
                "FROM information_schema.indexes AS ix "
                "WHERE ix.table_schema=:schema AND ix.table_name=:table "
                "AND ix.index_type in ('INDEX', 'UNIQUE')"
            ).bindparams(schema=schema, table=table_name)

        constraints = {}
        for const_name, is_unique in connection.execute(stmt):
            constraints[const_name] = {
                    'name': const_name, "column_names": [],
                    'unique': is_unique == 'YES'}

        stmt = text("SELECT ix.index_name, ic.column_name "
                "FROM information_schema.indexes AS ix "
                " JOIN information_schema.index_columns AS ic ON "
                    "ix.index_name=ic.index_name "
                    "AND ix.table_schema=ic.index_table_schema "
                    "WHERE ix.table_schema=:schema AND ix.table_name=:table "
                    "AND ix.index_type in ('INDEX', 'UNIQUE')"
                    "ORDER BY ic.ordinal_position"
                ).bindparams(schema=schema, table=table_name)

        for const_name, colname in connection.execute(stmt):
            constraints[const_name]["column_names"].append(colname)

        return list(constraints.values())
