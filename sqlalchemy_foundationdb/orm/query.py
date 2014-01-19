from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad
from ..dialect.base import NestedResult as _NestedResult, nested as _nested

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

    def __init__(self, query):
        stmt = query.statement
        super(orm_nested, self).__init__(stmt)
        self.type = ORMNestedResult(query)


@loader_option()
def nestedload(loadopt, attr):
    return loadopt.set_relationship_strategy(attr, {"lazy": "nested"})

@nestedload._add_unbound_fn
def nestedload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.nestedload, keys, False, {})

@nestedload._add_unbound_all_fn
def nestedload_all(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.nestedload, keys, True, {})
