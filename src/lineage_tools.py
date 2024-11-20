import json
import pandas as pd
import importlib
from src.utils import OracleAgent, classify_table_type_and_location
from modules.neo4jmodels import (
    config, 
    db, 
    # BItable, 
    # ERPtable, 
    # BIview, 
    # ERPview, 
    # LoadPlan, 
    # LoadPlanSE, 
    # LoadPlanPA,
    # Package,
    # Scenario
)
from src.type_enums import DBType, ObjectType, DBPrefix
from modules.queries import Queries


class LineageCronstructor:
    def __init__(self, configs) -> None:
        # self.db_type = db_type
        self.configs = configs
        config.DATABASE_URL = configs['neo4jdb']
        self.db = db


    def clean_all_nodes(self):
        delete_query = "MATCH (n) DETACH DELETE n"

        self.db.cypher_query(delete_query)

        print("All data has been deleted from the Neo4j database.")

    def check_all_nodes(self):
        query = """
        MATCH (a)-[r]-(b)
        RETURN a, r, b
        """

        cypher_results, meta = self.db.cypher_query(query)
        if cypher_results:
            results_as_dict = [dict(zip(meta, row)) for row in cypher_results]
            return results_as_dict
        else:
            return None


    def check_nodes_and_relationships(self, node_type, node_name, linked_node_name=None):
        query = f"""
        MATCH (a:{node_type})-[r]-(b)
        where a.name = $node_name
        {"AND b.name = $linked_node_name if linked_node_name else"}
        RETURN a, r, b
        """
        params = {"node_name": node_name}
        if linked_node_name:
            params["linked_node_name"] = linked_node_name
        
        cypher_results, meta = self.db.cypher_query(query, params)
        if cypher_results:
            # Return results as a list of dictionaries containing nodes and relationships
            results_as_dict = [dict(zip(meta, row)) for row in cypher_results]
            return results_as_dict
        else:
            return None


    def get_model_class(self, class_name):
        """
        Dynamically imports and returns the class from models.neo4jmodels.
        """
        try:
            module = importlib.import_module("modules.neo4jmodels")
            return getattr(module, class_name, None)
        except ImportError as e:
            raise ImportError(f"Error importing {class_name}: {e}")
        
        
    def get_object_class(self, target_name, table_type=None):
        """
        Return the class defined in ObjectType enum.

        If object type is unknow which means it could be Table or View.
        Otherwise the object type belongs to LoadPlan, Scenario or Interface.
        """

        if table_type == None:
            #TODO: refactor as a table/view classification function
            # db_type = DBPrefix.get_db_type(target_name).name
            # # if the table_name can be found in all_views then it's view, otherwise, table.
            # bi_sql_agent = OracleAgent(self.configs['BIDB_conn_info'])
            # biview_check_query = Queries.VIEWS_EQ.get_query(target_name)
            # bi_result = bi_sql_agent.read_table(biview_check_query)

            # # dataguard is the DB to store the data from ERP
            # dataguard_sql_agent = OracleAgent(self.configs['Data_guard'])
            # dataguard_check_query = Queries.VIEWS_EQ.get_query(target_name)
            # dataguard_result = dataguard_sql_agent.read_table(dataguard_check_query)

            # if not bi_result.empty or not dataguard_result.empty:
            #     table_type = 'View'
            # else:
            #     table_type = 'Table'

            db_type, table_type = classify_table_type_and_location(self.configs, target_name)

            db_table_type = db_type.upper() + table_type.lower()
            table_class = self.get_model_class(db_table_type)
            # table_class = globals().get(db_table_type)
            return table_class
        
        else:
            return self.get_model_class(table_type)



    def get_or_create_node(self, target_name, object_class=None, **identifier):
        """
        Check if the table name belongs to the object_type and return it if it exists.
        Create it if it does not exist.
        """
       
        object = self.get_object_class(target_name, object_class)
        object_node = object.nodes.get_or_none(**identifier)

        if object_node:
            return object_node
        else:
            return object(**identifier).save()
    

    def connect_nodes(self, source_node, target_node, process_rel=False):
        """
        Connects nodes dynamically based on their types (Table, View, BIview, ERPview, etc.).
        """
        source_labels = set(source_node.labels())
        target_labels = set(target_node.labels())

        if process_rel is False:
            if {'View', 'Table'} & source_labels and {'View', 'Table'} & target_labels:
                source_type = next(iter({'View', 'Table'} & source_labels))
                target_type = next(iter({'View', 'Table'} & target_labels))

                source_to_target_rel_name = f"parent_from_{source_type.lower()}"
                target_from_source_rel_name = f"child_to_{target_type.lower()}"

                # Dynamically connect the nodes
                getattr(source_node, source_to_target_rel_name).connect(target_node)
                getattr(target_node, target_from_source_rel_name).connect(source_node)

            elif 'LoadPlan' in source_labels and 'LoadPlan' in target_labels:
                source_node.previous_from.connect(target_node)
                target_node.next_to.connect(source_node)
            elif 'LoadPlan' in source_labels and 'Scenario' in target_labels:
                source_node.to_scenario.connect(target_node)
                target_node.from_loadplan.connect(source_node)
            elif 'Scenario' in source_labels and {'View', 'Table'} & target_labels:
                source_node.contains_table.connect(target_node)
                target_node.in_scenario.connect(source_node)

            else:
                raise ValueError(f"Unknown connection type between {source_labels} and {target_labels}")
        else:
            if {'View', 'Table'} & source_labels and {'View', 'Table'} & target_labels:
                source_type = next(iter({'View', 'Table'} & source_labels))
                target_type = next(iter({'View', 'Table'} & target_labels))

                source_to_target_rel_name = f"step_to_{source_type.lower()}"
                target_from_source_rel_name = f"step_from_{target_type.lower()}"

                # Dynamically connect the nodes
                getattr(source_node, source_to_target_rel_name).connect(target_node)
                getattr(target_node, target_from_source_rel_name).connect(source_node)



    # def creage_join_table_graphself(self, datasource_list, join_list):
    #     for join_rel in join_list:
    #         # creat join node
    #         parent_list = [table for table in datasource_list if table in join_rel]
    #         join_result_name = str(parent_list[0]) + " Join " + str(parent_list[1])

    #         # get the filter which comes from the same parent table
    #         join_result_node = self.get_or_create_node(join_result_name, 'JoinTable')

        
    #         # check if the join relationship is existed
    #         if join_result_node.join_condition == None or join_rel not in join_result_node.join_condition:
    #             # put all the join condition in the jointable node
    #             join_result_node.join_condition.append(join_rel)
    #             join_result_node.save()

    #         # create source table nodes
    #         joined_source_1_name = parent_list[0]
    #         joined_source_1_node = self.get_or_create_node(joined_source_1_name, 'SourceTable')
    #         joined_source_2_name = parent_list[1]
    #         joined_source_2_node = self.get_or_create_node(joined_source_2_name, 'SourceTable')

    #         # store the join condition in relationship
    #         join_source = [
    #             (joined_source_1_name, joined_source_1_node),
    #             (joined_source_2_name, joined_source_2_node)
    #         ]

    #         for source_name,  source_node in join_source:
    #             if source_name in join_rel:
    #                 join_result_node.join_from_base.connect(source_node, {'from_join': join_rel})


    def create_view_data_source(self, view_name, datasource_list):
        # create view node
        view_node = self.get_or_create_node(view_name, table_type=None)
        # view_node.syntax = syntax
        view_node.save()

        # create table node
        for table_name in datasource_list:
            table_node = self.get_or_create_node(table_name, table_type=None)  
            
            self.connect_nodes(view_node, table_node)      


    # Desconstruct all of the LLM result
    def result_destructure(self, view_name, input_string):
        # convert to dict
        input_dict = json.loads(input_string)

        # Convert the dictionary keys to a pandas Series 
        keys_series = pd.Series(input_dict.keys())

        if keys_series.str.contains('Union').any():
        
            for _, vlaue in input_dict.items():
                
                datasource_list = vlaue['Datasource']
                # filter_list = vlaue['Filter']
                # join_list = vlaue['Join']
                # groupby_dict = vlaue['Groupby']
            
                # here we call the function for desconstructing join_list, datasource_list, filter_list, groupby_dict
                self.create_view_data_source(view_name, datasource_list)

        else:
            datasource_list = input_dict['Datasource']
            # filter_list = input_dict['Filter']
            # join_list = input_dict['Join']
            # groupby_dict = input_dict['Groupby']

            self.create_view_data_source(view_name, datasource_list)

    def run(self, data):
        for _, row in data.iterrows():
            
            self.result_destructure(row.view_name, row.format_fixed_lineage)

    def get_or_create_scen_node(self, scen_name, scen_version):
        scen_identifier = {
            'name': scen_name,
            'scen_version': scen_version
        }
        return self.get_or_create_node(target_name=scen_name, object_class=ObjectType.Scenario.__str__(), **scen_identifier)
    
    def get_or_create_table_node(self, table_name):
        table_identifier = {'name': table_name}
        return self.get_or_create_node(target_name=table_name, object_class=None, **table_identifier)


