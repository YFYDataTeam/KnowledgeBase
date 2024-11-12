import pandas as pd
from models.queries import Queries
from src.utils import OracleAgent
from src.lineage_tools import LineageCronstructor
from src.type_enums import TableType
from models.neo4jmodels import LoadPlanPA, LoadPlanSE

class LoadPlanLineage(LineageCronstructor):

    def __init__(self, configs):
        super().__init__(configs=configs)
        

        # self.db_config = configs['ODI']
        # self.df_loadplan = self._get_loadplan_step(loadplan_id)
        
    # def _get_loadplan_step(self, loadplan_id)-> pd.DataFrame:

    #     loadplan_step = Queries.GET_LOADPLAN_STEP_TEST.get_query(loadplan_id)

    #     sql_agent = OracleAgent(config=self.db_config)
    #     df_loadplan = sql_agent.read_table(query=loadplan_step)

    #     return df_loadplan
    
    def create_loadplan_lineage(self, loadplan_table: pd.DataFrame):
        
        for _, row in loadplan_table.iterrows():
            
            loadplan_id = row['i_load_plan']
            lp_step = row['i_lp_step']
            lp_step_name = row['lp_step_name']
            par_lp_step = row['par_i_lp_step']
            lp_step_type = row['lp_step_type']

            # get or create root node(loadplan id) if par_i_lp_step is null
            if pd.isnull(par_lp_step):

                self.get_or_create_node(target_name=loadplan_id, table_class=LoadPlanSE, node_type=TableType.LoadPlan)

            else:
                node_type = TableType.LoadPlan.value + lp_step_type
                pa_node = self.get_or_create_node(target_name=lp_step_name, node_type=node_type)

            # get children step
            df_next_step = loadplan_table[loadplan_table['par_i_lp_step'] == lp_step]

            # PA indicates parallel process
            if lp_step_type == 'PA':
                node_class = TableType.LoadPlan.value + 'PA'
                self.get_or_create_node(target_name='Parallel'+'_'+{loadplan_id}, table_class=node_class, node_type=TableType.LoadPlan)
            
            if lp_step_type == 'RS':
                node_class = TableType.LoadPlan.value + 'RS'
                self.get_or_create_node(target_name=lp_step_name, table_class=node_class, node_type=TableType.LoadPlan)


                # self.connect_nodes(pa_node, node_class_next_step)

        


