import json
import pandas as pd

from src.lineage_tools import LineageCronstructor


# class LineageCronstructor:
#     def __init__(self, configs) -> None:
#         # self.db_type = db_type
#         self.configs = configs
#         config.DATABASE_URL = configs['neo4jdb']
#         self.db = db



class ViewLineageCreator(LineageCronstructor):
    def __init__(self, configs):
        super().__init__(configs)


    def connect_view_and_source(self, view_name, datasource_list):
        # create view node
        view_node = self.get_or_create_node(view_name)
        # view_node.syntax = syntax
        view_node.save()

        # create table node
        for table_name in datasource_list:
            table_node = self.get_or_create_node(table_name)  
            self.connect_nodes(view_node, table_node)      
            print(f"Successfully complete the lineage of view '{view_name}'.")


    # Desconstruct all of the LLM result
    def build_view_source_lineage(self, view_name, input):
        # convert to dict
        input_dict = json.loads(str(input))

        # Convert the dictionary keys to a pandas Series 
        keys_series = pd.Series(input_dict.keys())

        if keys_series.str.contains('Union').any():
        
            for _, vlaue in input_dict.items():
                
                datasource_list = vlaue['Datasource']
                # filter_list = vlaue['Filter']
                # join_list = vlaue['Join']
                # groupby_dict = vlaue['Groupby']
            
                # here we call the function for desconstructing join_list, datasource_list, filter_list, groupby_dict
                self.connect_view_and_source(view_name, datasource_list)
        else:
            datasource_list = input_dict['Datasource']
            # filter_list = input_dict['Filter']
            # join_list = input_dict['Join']
            # groupby_dict = input_dict['Groupby']

            self.connect_view_and_source(view_name, datasource_list)