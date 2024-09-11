from neomodel import (
    StructuredNode, 
    RelationshipTo, 
    RelationshipFrom, 
    StringProperty, 
    ArrayProperty,
    StructuredRel
)

class TableRel(StructuredRel):
    """
    Define the relationships between tables
    Store the multiple filters as an array
    """
    filter = ArrayProperty(StringProperty())


class BaseTable(StructuredNode):
    """
    The basic node for all tables
    """
    __label__ = 'Table'
    name = StringProperty(required=True, uniqued_index=True)
    comes_from = RelationshipFrom('BaseTable', "IS_FROM", model=TableRel)


class AggregatedTable(BaseTable):
    """
    The table which is aggregated by 'GROUP BY'
    """
    __label__ = 'Table'
    syntax = StringProperty(required=True)


class JoinTable(BaseTable):
    """
    Define the result that join by two tables
    """
    __label__ = 'Table'
    join_table_1 = RelationshipTo('BaseTable', 'JoinFrom', model=TableRel)
    join_table_2 = RelationshipTo('BaseTable', 'JoinFrom', model=TableRel)


class UnionTable(BaseTable):
    """
    Define the result comes from the tables that union together
    """
    __label__ = 'Table'
    from_base_table = RelationshipTo(BaseTable, 'FromBase', model=TableRel)
    from_aggregated_table = RelationshipTo(AggregatedTable, 'FromAggregated', model=TableRel)
    from_joined_table = RelationshipTo(JoinTable, 'FromJoin', model=TableRel)


class View(StructuredNode):
    """
    The node for View in the DB
    """
    __label__ = 'Table'
    name = StringProperty(unique_index=True)
    from_base_table = RelationshipTo(BaseTable, 'FROM_BASE', model=TableRel)
    from_aggregated_table = RelationshipFrom(AggregatedTable, 'FROM_GROUPBY', model=TableRel)
    from_joined_table = RelationshipFrom(JoinTable, 'FROM_JOIN', model=TableRel)
    from_union_table = RelationshipFrom(UnionTable, 'FROM_UNION', model=TableRel)