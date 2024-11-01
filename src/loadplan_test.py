import pandas as pd
from models.queries import Queries
from src.utils import OracleAgent




class LoadPlanLineage:
    def __init__(self, dbconfig, loadplan_id):
        self.db_config = dbconfig
        self.df_loadplan = self._get_loadplan_step(loadplan_id)

        pass
        
    def _get_loadplan_step(self, loadplan_id)-> pd.DataFrame:

        loadplan_step = Queries.GET_LOADPLAN_STEP_TEST.get_query(loadplan_id)

        sql_agent = OracleAgent(config=self.db_config)
        df_loadplan = sql_agent.read_table(query=loadplan_step)

        return df_loadplan
    
    def create_loadplan_lineage(self):



        pass


