import json
import pandas as pd
from src.utils import OracleAgent
from models.neo4jmodels import config, db, BI, ERP, JoinTable, AggregatTable, UnionTable, SourceTable
from src.commontypes import DBType, TableType
from models.queries import Queries


class LineageCronstructor:
    def __init__(self, configs) -> None:
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


    def table_tye_check(self, db_type, table_name):

        if db_type == DBType.BI:
            sql_agent = OracleAgent(self.configs['BIDB_conn_info'])
            query = Queries.BIDB_VIEWS_EQ(table_name)
            result = sql_agent(query)
            print(result)
        elif db_type == DBType.ERP:
            print(123)
        elif db_type == DBType.DW:
            print(12312)

        pass
        

    def get_node(self, table_name, table_type):
        """
        Check if the table name belongs to the table_type and return it if it exists.
        Create it if it does not exist.
        """

        table_type

        # Use getattr to dynamically access the class
        valid_type = TableType.get_table_type(table_name).name
        if not valid_type or valid_type != table_type:
            raise ValueError(f"Invalid or mismatched table type: {table_type} for table {table_name}")

        table_class = globals().get(table_type)
        
        if table_class:  # Ensure the class exists
            table = table_class.nodes.get_or_none(name=table_name)
            if table:
                return table
            else:
                return table_class(name=table_name, filter=[], join_condition=[]).save()
        else:
            raise ValueError(f"Unknown table type: {table_type}")
        

    def creage_join_table_graphself(self, datasource_list, join_list):
        for join_rel in join_list:
            # creat join node
            parent_list = [table for table in datasource_list if table in join_rel]
            join_result_name = str(parent_list[0]) + " Join " + str(parent_list[1])

            # get the filter which comes from the same parent table
            join_result_node = self.get_node(join_result_name, 'JoinTable')

        
            # check if the join relationship is existed
            if join_result_node.join_condition == None or join_rel not in join_result_node.join_condition:
                # put all the join condition in the jointable node
                join_result_node.join_condition.append(join_rel)
                join_result_node.save()

            # create source table nodes
            joined_source_1_name = parent_list[0]
            joined_source_1_node = self.get_node(joined_source_1_name, 'SourceTable')
            joined_source_2_name = parent_list[1]
            joined_source_2_node = self.get_node(joined_source_2_name, 'SourceTable')

            # store the join condition in relationship
            join_source = [
                (joined_source_1_name, joined_source_1_node),
                (joined_source_2_name, joined_source_2_node)
            ]

            for source_name,  source_node in join_source:
                if source_name in join_rel:
                    join_result_node.join_from_base.connect(source_node, {'from_join': join_rel})


    def create_view_data_source(self, view_name, datasource_list, syntax):
        # create view node
        view_node = self.get_node(view_name, 'View')
        view_node.syntax = syntax
        view_node.save()

        # create table node
        for table_name in datasource_list:
            table_node = self.get_node(table_name, 'SourceTable')  
            
            view_node.is_parent_from.connect(table_node)
            table_node.is_child_to.connect(view_node)


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



