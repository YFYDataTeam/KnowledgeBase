import pandas as pd
from src.type_enums import ObjectType
from src.lineage_tools import LineageCronstructor
from src.utils import OracleAgent, classify_table_type_and_location
from modules.sql import QueryManager


class ODILineageBuilder(LineageCronstructor):
    def __init__(self, configs, query_manager: QueryManager):
        super().__init__(configs)
        self.configs = configs
        self.qm = query_manager
        self.sql_agent = OracleAgent(config=configs['ODI'])

    def get_odi_etl_info(self, loadplan_id: int):
        """
        Retrieve ETL information for a specific load plan.
        """
        # Fetch load plan table
        lp_query = self.qm.get_loadplan_step_test
        loadplan_table = self.sql_agent.read_table(query=lp_query.format(loadplan_id=loadplan_id))

        # Fetch package and scenario steps
        scen_list = []
        for _, row in loadplan_table[loadplan_table['lp_step_type'] == 'RS'].iterrows():
            scen_dict = {
                row['scen_name']: row['scen_version']
            }
            scen_list.append(scen_dict)

        df_scenario_steps = pd.DataFrame()
        for item in scen_list:
            scen_name = list(item.keys())[0]
            scen_version = item[scen_name]

            # Get scenario ID
            scenario_id_query = self.qm.get_scenario_id
            pkg_table = self.sql_agent.read_table(
                query=scenario_id_query.format(
                    scen_name=f"'{scen_name}'", scen_version=f"'{scen_version}'"
                )
            )

            scen_no = pkg_table.scen_no.iloc[0]

            # Fetch scenario steps
            scenario_step_query = self.qm.get_scenario_steps
            df_scen_steps = self.sql_agent.read_table(query=scenario_step_query.format(scen_no=scen_no))
            df_scen_steps['scen_name'] = scen_name
            df_scen_steps['scen_version'] = scen_version

            df_scenario_steps = pd.concat([df_scenario_steps, df_scen_steps], axis=0)

        return loadplan_table, df_scenario_steps

    def create_previous_node(
        self, 
        source_table: pd.DataFrame, 
        source_table_uni_col: str, 
        source_table_uni_id: str, 
        search_col: str, 
        pre_step_id: str,
    ):
        """
        Create relationships between the current step and its previous step.
        """
        df_related_steps = source_table[source_table[search_col] == pre_step_id]

        for _, row in df_related_steps.iterrows():
            if row['par_i_lp_step'] is None:
                # Create or get root-level relationship to LoadPlan node
                previous_step_identifier = {
                    'name': f"LP_{source_table_uni_id}",
                    'loadplan_id': source_table_uni_id
                }
                previous_node = self.get_or_create_node(
                    target_name=previous_step_identifier['name'], 
                    object_class=ObjectType.LoadPlan.__str__(), 
                    **previous_step_identifier
                )

            elif row['lp_step_type'] == 'PA':
                previous_step_identifier = {
                    'name': f"PA_{pre_step_id}",
                    'step_id': pre_step_id
                }
                previous_node = self.get_or_create_node(
                    target_name=previous_step_identifier['name'], 
                    object_class=ObjectType.LoadPlan.__str__(), 
                    **previous_step_identifier
                )

            elif row['lp_step_type'] == 'RS':
                pass
            
            return previous_node

    def create_loadplan_lineage(self, loadplan_table: pd.DataFrame):
        """
        Create the lineage for the load plan steps.
        """
        loadplan_table['i_lp_step'] = loadplan_table['i_lp_step'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        loadplan_table['par_i_lp_step'] = loadplan_table['par_i_lp_step'].apply(lambda x: None if pd.isnull(x) else str(int(x)))
        for _, row in loadplan_table.iterrows():
            loadplan_id = row['i_load_plan']
            lp_step = row['i_lp_step']
            lp_step_name = row['lp_step_name']
            par_lp_step =  row['par_i_lp_step']
            lp_step_type = row['lp_step_type']

            if par_lp_step is None:
                # Create root node
                target_name = f"LP_{loadplan_id}"
                identifier = {
                    'name': target_name,
                    'loadplan_id': loadplan_id
                }
                self.get_or_create_node(target_name=identifier['name'], object_class=ObjectType.LoadPlan.__str__(), **identifier)

            if lp_step_type == 'PA':
                # Create parallel processing node
                target_name = f"PA_{lp_step}"
                identifier = {
                    'name': target_name,
                    'step_id': lp_step
                }
                cur_node = self.get_or_create_node(target_name=identifier['name'], object_class=ObjectType.LoadPlan.__str__() + 'PA', **identifier)

                pre_node = self.create_previous_node(
                    source_table=loadplan_table,
                    source_table_uni_col='i_load_plan',
                    source_table_uni_id=loadplan_id,
                    search_col='i_lp_step',
                    pre_step_id=par_lp_step
                )

                self.connect_nodes(pre_node, cur_node)

            if lp_step_type == 'RS':
                # Create package/scenario node
                target_name = f"RS_{lp_step}"
                identifier = {
                    'name': target_name,
                    'step_id':lp_step, 
                    'scen_name': lp_step_name
                }

                cur_rs_node = self.get_or_create_node(target_name=identifier['name'], object_class=ObjectType.LoadPlan.__str__() + 'RS', **identifier)

                pre_node = self.create_previous_node(
                    source_table=loadplan_table,
                    source_table_uni_col='i_load_plan',
                    source_table_uni_id=loadplan_id,
                    search_col='i_lp_step',
                    pre_step_id=par_lp_step
                )
                self.connect_nodes(pre_node, cur_rs_node)

                # get or create scenario node
                scen_name = row['scen_name']
                scen_version = row['scen_version']
                scen_identifier = {
                    'name': scen_name,
                    'scen_version': scen_version
                }

                scen_node = self.get_or_create_node(target_name=scen_name, object_class=ObjectType.Scenario.__str__(), **scen_identifier)

                # link loadplan with scenario node
                self.connect_nodes(cur_rs_node, scen_node)

    def create_scenario_lineage(self, scenario_steps: pd.DataFrame):

        # link LoadPlanRS node with scenario node
        scen_list = scenario_steps.scen_name.unique().tolist()


        for _, row in scenario_steps.iterrows():

            scen_name = row['scen_name']
            scen_version = row['scen_version']
            scen_identifier = {
                'name': scen_name,
                'scen_version': scen_version
            }

            # get or create scenario node
            scen_node = self.get_or_create_node(target_name=scen_name, object_class=ObjectType.Scenario.__str__(), **scen_identifier)

            # get or create table node after classification
            table_name = row['table_name']
            table_identifier = {
                'name': table_name
            }
            table_node = self.get_or_create_node(target_name=table_name, object_class=None, **table_identifier)
            # link scenario and table
            self.connect_nodes(scen_node, table_node)



        return 1

    def create_odi_lineage(self, loadplan_id: int):
        """
        Main entry point to create lineage for a given LoadPlan ID.
        """
        loadplan_table, scenario_steps = self.get_odi_etl_info(loadplan_id)

        # Create LoadPlan lineage
        self.create_loadplan_lineage(loadplan_table)

        # Create Scenario lineage
        self.create_scenario_lineage(scenario_steps)

        # Create Scenario lineage (if applicable)
        # Add logic here if needed to process df_scenario_steps

        print("Lineage creation completed.")
