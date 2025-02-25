import os
from modules.queries import Queries
from src.models import JobType, LineageType, DBType, LlmType
from src.sqlparser import SQLParser
from tests.test_cases import BIDB_TEST_CASES
from src.lineage_tools import LineageCronstructor
from src.view_lineage import ViewLineageCreator
from src.utils import OracleAgent
from modules.sql import QueryManager

from src.odi_lineage import ODILineageBuilder

class JobDispatcher:
    def __init__(self, configs, sql_dir):
        self.configs = configs
        self.qm = QueryManager(sql_dir)

    def run_job(self, job_type):

        if job_type == JobType.ERPTOBI.value:

            create_erp_to_bidb_data_lineage(self.configs)

            query = Queries.ERP_TO_BI_TEST_CASE.value

            create_erp_views_lineage(self.configs, query, llm_type=LlmType.AOAI)


        elif job_type == JobType.LOADPLAN.value:
            
            odi_lineage_builder = ODILineageBuilder(self.configs, self.qm,)
            odi_lineage_builder.create_odi_lineage(loadplan_id='45502')

        elif job_type == JobType.ERPVIEWS.value:

            query = Queries.ERP_ORDER.value

            create_erp_views_lineage(self.configs, query, llm_type=LlmType.AOAI)

        else: 
            raise ValueError({f'Unknown job type: {job_type}'})


def create_erp_to_bidb_data_lineage(configs):
    """
    Since the relationship is orgainzed in a table, LLM deconstruction is not needed.
    """
    dbconfig = configs['BIDB_conn_info']
    query = Queries.ODI_TEST_CASE.value

    sql_agent = OracleAgent(config=dbconfig)
    erp_to_bidb_relationship_data = sql_agent.read_table(query=query)

    lineage_agent = LineageCronstructor(configs)

    # for testing, delete all nodes at first
    # lineage_agent.clean_all_nodes()

    for _, row in erp_to_bidb_relationship_data.iterrows():
        target_identifier = {
            'name': row['target_table']
        }
        target_node = lineage_agent.get_or_create_node(target_name=row.target_table, **target_identifier)

        source_identifier = {
            'name': row['source_table']
        }
        source_node = lineage_agent.get_or_create_node(target_name=row.source_table, **source_identifier)

        lineage_agent.connect_nodes(target_node, source_node)        


def create_erp_views_lineage(configs, query, llm_type):

    dbconfig = configs['Data_guard']

    sql_agent = OracleAgent(config=dbconfig)
    erp_views = sql_agent.read_table(query=query)

    relationship_type = LineageType.DataSourceOnly

    sql_parser= SQLParser(configs, llm_type)

    desconstructed_sql = sql_parser.parse_datasource(row, parse_type='re')

    lineage_agent = ViewLineageCreator(configs)
    
    for _, row in desconstructed_sql.iterrows():
        
        lineage_agent.result_destructure(row.view_name, row.format_fixed_lineage)



