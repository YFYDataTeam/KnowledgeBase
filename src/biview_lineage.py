import pandas as pd
from src.type_enums import LineageType
from src.sql_deconstruction import SQLDeconstructor
from tests.test_cases import BIDB_TEST_CASES
from src.view_lineage import ViewLineageCreator
from src.utils import OracleAgent


def create_bidb_views_lineage(configs, query, llm_type):

    # if test_case:
    #     input_data = input_data[input_data['view_name'].isin(test_case)]

    sql_agent = OracleAgent(configs['BIDB_conn_info'])
    input_data = sql_agent.read_table(query=query)

    view_lineage_agent = ViewLineageCreator(configs)

    desconstruted_sql_list = []
    for _, row in input_data.iterrows():
        target = row['view_name']
        result = view_lineage_agent.check_node(target=row['view_name'], label='BIview', property='name')

        if result is None:

            relationship_type = LineageType.DataSourceOnly

            sql_deconstructor = SQLDeconstructor(configs, llm_type)

            desconstructed_sql = sql_deconstructor.run(row, relationship_type)

            desconstruted_sql_list.append(desconstructed_sql)

            df_desconstruted_result = pd.DataFrame(desconstruted_sql_list)
        
        else:
            print(f'{target} is existed.')
            continue
    

    for _, row in df_desconstruted_result.iterrows():
        try:
            print(f"Building nodes for: '{row['view_name']}'.")
            view_lineage_agent.build_view_source_lineage(row['view_name'], row['format_fixed_lineage'])
        except Exception as e:
            print(f"'{row['view_name']}' is problematic when building lineages. Error: {e}")
            continue



    # Problematic views: W_CHP_WIP_BATCH_PERC_V

