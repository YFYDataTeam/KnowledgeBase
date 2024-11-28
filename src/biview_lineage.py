import pandas as pd
from src.sqlparser import SQLParser
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

            sql_parser = SQLParser(configs, llm_type)

            parse_result = sql_parser.parse_datasource(row, parse_type='re')

            desconstruted_sql_list.append(parse_result)

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

