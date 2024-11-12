from models.queries import Queries
from src.type_enums import JobType, LineageType, DBType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from tests.test_cases import BIDB_TEST_CASES
from src.lineage_tools import LineageCronstructor
from src.loadplan_lineage import LoadPlanLineage
from src.utils import OracleAgent


class JobDispatcher:
    def __init__(self, configs):
        self.configs = configs
    
    def run_job(self, job_type):
        if job_type == JobType.BIVIEWS:
            create_bidb_views_lineage(self.configs, llm_type=LlmType.AOAI)
        elif job_type == JobType.ERPTOBI:

            create_erp_to_bidb_data_lineage(self.configs)

            create_erp_views_lineage(self.configs, llm_type=LlmType.AOAI)

            print('done')
        elif job_type == JobType.LOADPLAN:
            
            create_loadplan_lineage(self.configs, loadplan_id='111502')

        else: 
            raise ValueError({f'Unknown job type: {job_type}'})


def create_bidb_views_lineage(configs, llm_type):

    query = Queries.BIDB_TEST_QUERY.value
    test_case = BIDB_TEST_CASES


    sql_agent = OracleAgent(configs['BIDB_conn_info'])
    input_data = sql_agent.read_table(query=query)

    if test_case:
        input_data = input_data[input_data['view_name'].isin(test_case)]

    relationship_type = LineageType.DataSourceOnly

    sql_deconstructor = SQLDeconstructor(configs, llm_type)

    desconstructed_sql = sql_deconstructor.run(input_data, relationship_type)

    lineage_agent = LineageCronstructor(configs)

    lineage_agent.run(desconstructed_sql)

    # for testing, delete all nodes at first
    # lineage_agent.clean_all_nodes()
    

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

        target_node = lineage_agent.get_node(row.target_table, table_type=None)
        source_node = lineage_agent.get_node(row.source_table, table_type=None)

        lineage_agent.connect_nodes(target_node, source_node)        


def create_erp_views_lineage(configs, llm_type):

    dbconfig = configs['Data_guard']
    
    query = Queries.ERP_TO_BI_TEST_CASE.value

    sql_agent = OracleAgent(config=dbconfig)
    erp_views = sql_agent.read_table(query=query)

    relationship_type = LineageType.DataSourceOnly

    sql_deconstructor = SQLDeconstructor(configs, llm_type)

    desconstructed_sql = sql_deconstructor.run(erp_views, relationship_type)

    lineage_agent = LineageCronstructor(configs)
    
    #TODO: refactor
    lineage_agent.run(desconstructed_sql)



def create_loadplan_lineage(config, loadplan_id):

    query = Queries.GET_LOADPLAN_STEP_TEST.value

    sql_agent = OracleAgent(config=config['ODI'])
    loadlplan_table = sql_agent.read_table(query=query.format(loadplan_id=loadplan_id))
    
    loadplan_agent = LoadPlanLineage(config)

    loadplan_agent.create_loadplan_lineage(loadplan_table=loadlplan_table)



    return print('done')