from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.exclusions import \
    skip_if, \
    SpecPredicate


def exclude(db, op, spec, description=None):
    return SpecPredicate(db, op, spec, description=description)

class Requirements(SuiteRequirements):
    @property
    def foreign_key_ddl(self):
        return exclusions.open()

    @property
    def self_referential_foreign_keys(self):
        return exclusions.open()

    @property
    def table_reflection(self):
        return exclusions.open()

    @property
    def unique_constraint_reflection(self):
        return exclusions.open()

    @property
    def views(self):
        return exclusions.open()

    @property
    def view_column_reflection(self):
        return exclusions.open()

    @property
    def view_reflection(self):
        return exclusions.open()

    @property
    def schema_reflection(self):
        return exclusions.open()

    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return exclusions.closed()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return skip_if([
            exclude('foundationdb', '<=', (1,9,5), 'primary key names not preserved'),
        ])

    @property
    def primary_key_constraint_reflection(self):
        return skip_if([
            exclude('foundationdb', '<=', (1,9,5), 'primary key names not preserved'),
        ])

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.open()

    @property
    def index_reflection(self):
        return exclusions.open()

    @property
    def returning(self):
        return exclusions.open()


    @property
    def text_type(self):
        """Target database must support an unbounded Text() "
        "type such as TEXT or CLOB"""
        return exclusions.open()

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""

        return exclusions.open()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        # foundationdb doesn't seem to support this
        return exclusions.closed()

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.closed()

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.open()

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.open()

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1900) values."""

        return exclusions.open()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.open()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        # foundationdb only allows precision up to 31 digits
        return exclusions.closed()

    @property
    def duplicate_names_in_cursor_description(self):
        # for result sets that don't have nested=True (and none of the
        # SQLAlchemy suite tests do), we are OK with this.
        return exclusions.open()

    @property
    def percent_schema_names(self):
        return exclusions.closed()
