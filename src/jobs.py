from models.queries import Queries
from src.commontypes import JobType, LineageType, TableType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from tests.test_cases import BIDB_TEST_CASES
from src.lineage_tools import LineageCronstructor
from src.utils import OracleAgent


class JobDispatcher:
    def __init__(self, configs):
        self.configs = configs
    
    def run_job(self, job_type):
        if job_type == JobType.BIVIEWS:
            create_bidb_views_lineage(self.configs, llm_type=LlmType.AOAI)
        elif job_type == JobType.ERPTOBI:
            # create_erp_to_bidb_data_lineage(self.configs)

            create_erp_views_lineage(self.configs, llm_type=LlmType.AOAI)

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
    # desconstructed_sql.to_csv('./result/auto_result1.csv')

    lineage_agent = LineageCronstructor(configs)

    # for testing, delete all nodes at first
    # lineage_agent.clean_all_nodes()
    



def create_erp_to_bidb_data_lineage(configs):
    """
    Since the relationship is orgainzed in a table, LLM is not needed.
    """
    dbconfig = configs['BIDB_conn_info']
    query = Queries.ODI_TEST_CASE.value

    sql_agent = OracleAgent(config=dbconfig)
    erp_to_bidb_relationship_data = sql_agent.read_table(query=query)

    lineage_agent = LineageCronstructor(configs)

    # for testing, delete all nodes at first
    # lineage_agent.clean_all_nodes()

    for _, row in erp_to_bidb_relationship_data.iterrows():
        target_type = TableType.get_table_type(row.target_table.upper()).name
        source_type = TableType.get_table_type(row.source_table.upper()).name

        target_node = lineage_agent.get_node(row.target_table, target_type)
        source_node = lineage_agent.get_node(row.source_table, source_type)

        target_node.child_to.connect(source_node)
        source_node.parent_from.connect(target_node)

    print('0')


def create_erp_views_lineage(configs, llm_type):

    dbconfig = configs['Data_guard']
    
    query = Queries.ERP_TO_BI_TEST_CASE.value

    sql_agent = OracleAgent(config=dbconfig)
    erp_views = sql_agent.read_table(query=query)

    relationship_type = LineageType.DataSourceOnly

    sql_deconstructor = SQLDeconstructor(configs, llm_type)

    desconstructed_sql = sql_deconstructor.run(erp_views, relationship_type)

    lineage_agent = LineageCronstructor(configs)

    

    return