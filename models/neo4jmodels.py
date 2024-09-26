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

    from_source = StringProperty()
    from_join = StringProperty()
    from_aggregate = StringProperty()

    union_source = StringProperty()
    union_join = StringProperty()
    union_aggregate = StringProperty()

class SourceTable(StructuredNode):
    """
    The basic node for all tables
    """

    name = StringProperty(uniqued_index=True)
    filter = ArrayProperty(StringProperty())
    join_condition = ArrayProperty(StringProperty())
    # child_to_view = Relationship('View', 'ChildtoView')
    child_to = Relationship('SourceTable', 'Child', model=TableRel)

    parent_from = RelationshipTo('SourceTable', 'Parent', model=TableRel)

class BIsource(SourceTable):
    pass

class ERP_source(SourceTable):
    pass

class JoinTable(SourceTable):
    """ 
    Define the result that join by two tables
    """

    join_from_source = RelationshipFrom('SourceTable', 'JoinFromsource', model=TableRel)

    join_from_join = RelationshipFrom('JoinTable', 'JoinFromJoin', model=TableRel)

    join_from_aggregate = RelationshipFrom('AggregatTable', 'JoinFromAggregated', model=TableRel)

    join_from_union = RelationshipFrom('UnionTable', 'JoinFromUnion', model=TableRel)


class AggregatTable(SourceTable):
    """
    The table which is aggregated by 'GROUP BY'
    """

    syntax = StringProperty(required=True)
    
    aggregate_from_source = RelationshipFrom('SourceTable', 'AggregateFromsource')
    aggregate_from_join = RelationshipFrom('JoinTable', 'AggregateFromJoin')
    aggregate_from_aggregate = RelationshipFrom('AggregatTable', 'AggregateFromAggregate')
    aggregate_from_union = RelationshipFrom('UnionTable', 'AggregateFromUnion')

class UnionTable(SourceTable):
    """
    Represents the result of union operations between tables.

    This class is necessary because the union result may later be aggregated 
    or used in further operations.
    """

    from_source_table = RelationshipTo(SourceTable, 'Fromsource', model=TableRel)
    from_aggregated_table = RelationshipTo(AggregatTable, 'FromAggregated', model=TableRel)
    from_joined_table = RelationshipTo(JoinTable, 'FromJoin', model=TableRel)

class View(StructuredNode):
    """
    The node for View in the DB
    """
    name = StringProperty(unique_index=True)
    syntax = StringProperty()

    # is_parent_from = RelationshipTo(SourceTable, 'IsParentOf', model=TableRel)
    from_aggregated_table = RelationshipFrom(AggregatTable, 'FromGroupby', model=TableRel)
    from_joined_table = RelationshipFrom(JoinTable, 'FromJoin', model=TableRel)
    from_union_table = RelationshipFrom(UnionTable, 'FROM_UNION', model=TableRel)

class BIview(View):
    pass

class ERPview(View):
    pass
