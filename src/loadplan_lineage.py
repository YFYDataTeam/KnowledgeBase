import pandas as pd
from models.queries import Queries
from src.utils import OracleAgent
from src.lineage_tools import LineageCronstructor
from src.type_enums import TableType


class LoadPlanLineage(LineageCronstructor):

    def __init__(self, configs, loadplan_id):
        super.__init__(self,configs=configs)

        self.db_config = configs['ODI']
        self.df_loadplan = self._get_loadplan_step(loadplan_id)

        pass
        
    def _get_loadplan_step(self, loadplan_id)-> pd.DataFrame:

        loadplan_step = Queries.GET_LOADPLAN_STEP_TEST.get_query(loadplan_id)

        sql_agent = OracleAgent(config=self.db_config)
        df_loadplan = sql_agent.read_table(query=loadplan_step)

        return df_loadplan
    
    def create_loadplan_lineage(self):

        
        for _, row in self.df_loadplan.iterrows():
            
            loadplan_id = row['I_LOAD_PLAN']
            lp_step = row['I_LP_STEP']
            lp_step_name = row['LP_STEP_NAME']
            par_lp_step = row['PAR_I_LP_STEP']
            lp_step_type = row['LP_STEP_TYPE']

            # get or create first node(loadplan id)
            if pd.isnull(par_lp_step):

                self.get_or_create_node(target_name=loadplan_id, node_type = TableType.LoadPlan)

            node_type = TableType.LoadPlan.value + lp_step_type
            node = self.get_or_create_node(target_name=lp_step_name, node_type=node_type)

            # get children step
            df_next_step = self.df_loadplan[self.df_loadplan['PAR_I_LP_STEP'] == lp_step]
            for _, next_step_row in df_next_step.iterrows():
                # PA indicates parallel process
                if next_step_row['LP_STEP_TYPE'] == 'PA':
                    next_node_type = TableType.LoadPlan.value + 'PA'
                    self.connect_nodes()

        


