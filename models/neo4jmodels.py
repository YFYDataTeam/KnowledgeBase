from neomodel import (
    config,
    db,
    StructuredNode, 
    RelationshipTo, 
    RelationshipFrom, 
    Relationship,
    StringProperty, 
    ArrayProperty,
    StructuredRel
)

class TableRel(StructuredRel):
    """
    Define the relationships between tables
    Store the multiple filters as an array
    """

    from_base = RelationshipFrom('BaseTable', "FromBase")
    from_join = RelationshipFrom('JoinTable', "FromJoin")
    from_aggregate = RelationshipFrom('JoinTable', "FromAggregate")

    union_base = Relationship('BaseTable', 'UnionBase')
    union_join = Relationship('BaseTable', 'UnionJoin')
    union_aggregate = Relationship('BaseTable', 'UnionAggregate')

class BaseTable(StructuredNode):
    """
    The basic node for all tables
    """

    name = StringProperty(uniqued_index=True)
    filter = ArrayProperty(StringProperty())

class JoinTable(BaseTable):
    """
    Define the result that join by two tables
    """

    join_from_base_1 = RelationshipFrom('BaseTable', 'JoinFromBase', model=TableRel)
    join_from_base_2 = RelationshipFrom('BaseTable', 'JoinFromBase', model=TableRel)

    join_from_join_1 = RelationshipFrom('JoinTable', 'JoinFromJoin', model=TableRel)
    join_from_join_2 = RelationshipFrom('JoinTable', 'JoinFromJoin', model=TableRel)

    join_from_aggregate_1 = RelationshipFrom('AggregatTable', 'JoinFromAggregated', model=TableRel)
    join_from_aggregate_2 = RelationshipFrom('AggregatTable', 'JoinFromAggregated', model=TableRel)

    join_from_union_1 = RelationshipFrom('UnionTable', 'JoinFromUnion', model=TableRel)
    join_from_union_2 = RelationshipFrom('UnionTable', 'JoinFromUnion', model=TableRel)

class AggregatTable(BaseTable):
    """
    The table which is aggregated by 'GROUP BY'
    """

    syntax = StringProperty(required=True)
    
    aggregate_from_base = RelationshipFrom('BaseTable', 'AggregateFromBase')
    aggregate_from_join = RelationshipFrom('JoinTable', 'AggregateFromJoin')
    aggregate_from_aggregate = RelationshipFrom('AggregatTable', 'AggregateFromAggregate')
    aggregate_from_union = RelationshipFrom('UnionTable', 'AggregateFromUnion')

class UnionTable(BaseTable):
    """
    Represents the result of union operations between tables.

    This class is necessary because the union result may later be aggregated 
    or used in further operations.
    """

    from_base_table = RelationshipTo(BaseTable, 'FromBase', model=TableRel)
    from_aggregated_table = RelationshipTo(AggregatTable, 'FromAggregated', model=TableRel)
    from_joined_table = RelationshipTo(JoinTable, 'FromJoin', model=TableRel)

class View(StructuredNode):
    """
    The node for View in the DB
    """
    name = StringProperty(unique_index=True)

    from_base_table = RelationshipFrom(BaseTable, 'FromBase', model=TableRel)
    from_aggregated_table = RelationshipFrom(AggregatTable, 'FromGroupby', model=TableRel)
    from_joined_table = RelationshipFrom(JoinTable, 'FromJoin', model=TableRel)
    from_union_table = RelationshipFrom(UnionTable, 'FROM_UNION', model=TableRel)