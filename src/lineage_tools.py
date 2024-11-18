import json
import pandas as pd
import importlib
from src.utils import OracleAgent
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


    def check_nodes_and_relationships(self, table_type):
        query = f"""
        MATCH (a:{table_type})-[r]-(b)
        RETURN a, r, b
        """
        
        cypher_results, meta = self.db.cypher_query(query)
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
        
        
    def get_object_class(self, table_name, object_class=None):
        """
        Return the class defined in ObjectType enum.

        If object type is unknow which means it could be Table or View.
        Otherwise the object type belongs to LoadPlan, Scenario or Interface.
        """

        if object_class == None:
            db_type = DBPrefix.get_db_type(table_name).name
            # if the table_name can be found in all_views then it's view, otherwise, table.
            bi_sql_agent = OracleAgent(self.configs['BIDB_conn_info'])
            biview_check_query = Queries.VIEWS_EQ.get_query(table_name)
            bi_result = bi_sql_agent.read_table(biview_check_query)

            # dataguard is the DB to store the data from ERP
            dataguard_sql_agent = OracleAgent(self.configs['Data_guard'])
            dataguard_check_query = Queries.VIEWS_EQ.get_query(table_name)
            dataguard_result = dataguard_sql_agent.read_table(dataguard_check_query)

            if not bi_result.empty or not dataguard_result.empty:
                object_class = 'View'
            else:
                object_class = 'Table'

            db_table_type = db_type.upper() + object_class.lower()
            table_class = self.get_model_class(db_table_type)
            # table_class = globals().get(db_table_type)
            return table_class
        
        else:
            return self.get_model_class(object_class)



    def get_or_create_node(self, target_name, object_class=None):
        """
        Check if the table name belongs to the object_type and return it if it exists.
        Create it if it does not exist.
        """
       
        # object_class is None which means we don't sure it's View or Table.
        object = self.get_object_class(target_name, object_class)
        object_node = object.nodes.get_or_none(name=target_name)

        if object_node:
            return object_node
        else:
            return object(name=target_name).save()
    

    def connect_nodes(self, target_node, source_node):
        """
        Connects nodes dynamically based on their types (Table, View, BIview, ERPview, etc.).
        """
        source_labels = source_node.labels()
        target_labels = target_node.labels()

        if ['View','Table'] in target_labels and ['View','Table'] in source_labels:
            target_node.child_to_table.connect(source_node)
            source_node.parent_from_view.connect(target_node)
        elif ['LoadPlan'] in source_labels:
            target_node.next_to.connect(source_node)
            source_node.previous_from.connect(target_node)

        else:
            raise ValueError(f"Unknown connection type between {source_labels} and {target_labels}")


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



