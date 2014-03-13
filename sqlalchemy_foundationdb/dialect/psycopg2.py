"""Implement the FoundationDB dialect via an extended version of the psycopg2
driver.

"""
from __future__ import absolute_import

from .base import FDBDialect, FDBExecutionContext, FDBCompiler

from fdb_sql import psycopg2 as fdb_psycopg2


class FDBPsycopg2ExecutionContext(FDBExecutionContext):
    def create_cursor(self):
        return self._dbapi_connection.cursor(nested=not self.is_crud)


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
