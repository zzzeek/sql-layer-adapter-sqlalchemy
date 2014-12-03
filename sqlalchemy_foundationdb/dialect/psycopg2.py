"""Implement the FoundationDB dialect via an extended version of the psycopg2
driver.

"""
from __future__ import absolute_import

from .base import FDBDialect, FDBExecutionContext, FDBCompiler

from foundationdb_sql import psycopg2 as fdb_psycopg2


class FDBPsycopg2ExecutionContext(FDBExecutionContext):
    def create_cursor(self):
        nested = self.execution_options.get('foundationdb_nested', False) or (
                not self.isddl and self.compiled and
                self.compiled._foundationdb_nested
            )
        return self._dbapi_connection.cursor(nested=nested)


class FDBPsycopg2Compiler(FDBCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
                self.process(binary.right, **kw)

    def post_process_text(self, text):
        return text.replace('%', '%%')

class FDBPsycopg2Dialect(FDBDialect):
    use_native_unicode = True

    statement_compiler = FDBPsycopg2Compiler
    execution_ctx_cls = FDBPsycopg2ExecutionContext
    driver = 'psycopg2'

    supports_native_decimal = True

    @classmethod
    def dbapi(cls):
        import psycopg2
        return psycopg2

    def on_connect(self):
        fns = []

        # TODO: not sure what the unicode situation is yet
        if self.dbapi and self.use_native_unicode:
            from psycopg2 import extensions
            def setup_unicode_extension(conn):
                extensions.register_type(extensions.UNICODE, conn)
            fns.append(setup_unicode_extension)

        if fns:
            def on_connect(conn):
                for fn in fns:
                    fn(conn)
            return on_connect
        else:
            return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        opts['connection_factory'] = fdb_psycopg2.Connection
        return ([], opts)

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.Error):
            # check the "closed" flag.  this might not be
            # present on old psycopg2 versions
            if getattr(connection, 'closed', False):
                return True

            # legacy checks based on strings.  the "closed" check
            # above most likely obviates the need for any of these.
            str_e = str(e).partition("\n")[0]
            for msg in [
                # these error messages from libpq: interfaces/libpq/fe-misc.c
                # and interfaces/libpq/fe-secure.c.
                'terminating connection',
                'closed the connection',
                'connection not open',
                'could not receive data from server',
                'could not send data to server',
                # psycopg2 client errors, psycopg2/conenction.h,
                # psycopg2/cursor.h
                'connection already closed',
                'cursor already closed',
                # not sure where this path is originally from, it may
                # be obsolete.   It really says "losed", not "closed".
                'losed the connection unexpectedly',
                # this can occur in newer SSL
                'connection has been closed unexpectedly'
            ]:
                idx = str_e.find(msg)
                if idx >= 0 and '"' not in str_e[:idx]:
                    return True
        return False
