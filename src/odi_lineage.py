import pandas as pd
from src.type_enums import JobType, LineageType, DBType, LlmType
from src.lineage_tools import LineageCronstructor
from src.view_lineage import ViewLineageCreator
from src.loadplan_lineage import LoadPlanLineage
from src.utils import OracleAgent
from models.sql import QueryManager




def create_loadplan_lineage(config, qm, loadlplan_table):
    query = qm.get_loadplan_step_test

    # query = Queries.GET_LOADPLAN_STEP_TEST.value


    
    loadplan_agent = LoadPlanLineage(config)

    loadplan_agent.create_loadplan_lineage(loadplan_table=loadlplan_table)



    return print('done')



def get_etl_info(config, qm, loadplan_id):
    sql_agent = OracleAgent(config=config['ODI'])
    lp_query = qm.get_loadplan_step_test

    loadlplan_table = sql_agent.read_table(query=lp_query.format(loadplan_id=loadplan_id))

    # package dict
    scen_list = []
    for _, row in loadlplan_table[loadlplan_table['lp_step_type'] == 'RS'].iterrows():
        scen_dict = {}
        loadplan_id = row['i_load_plan']
        scen_name = row['scen_name']
        scen_version = row['scen_version']

        scen_dict[scen_name] = scen_version

        scen_list.append(scen_dict)


    df_scenario_steps = pd.DataFrame()
    for item in scen_list:
        # note the scenario are the version of package.
        scenario_id_query = qm.get_scenario_id
        scen_name = list(item.keys())[0]
        scen_version = item[scen_name]
        pkg_table = sql_agent.read_table(query=scenario_id_query.format(scen_name=f"'{scen_name}'", scen_version=f"'{scen_version}'"))

        scen_no = pkg_table.scen_no.iloc[0]

        scenario_step_query = qm.get_scenario_steps
        df_scen_steps = sql_agent.read_table(query=scenario_step_query.format(scen_no=scen_no))

        df_scen_steps['scen_name'] = scen_name

        df_scenario_steps = pd.concat([df_scenario_steps, df_scen_steps], axis=0)

    return loadlplan_table, df_scenario_steps


def create_odi_lineage(config, qm, loadplan_id):

    loadlplan_table, df_scenario_steps = get_etl_info(config, qm, loadplan_id)


    return 