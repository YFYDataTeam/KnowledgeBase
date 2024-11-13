import pandas as pd
from src.type_enums import ObjectType
from src.lineage_tools import LineageCronstructor
from src.view_lineage import ViewLineageCreator
from src.loadplan_lineage import LoadPlanLineage
from src.utils import OracleAgent
from models.sql import QueryManager

from models.neo4jmodels import LoadPlanSE



def create_loadplan_lineage(config, loadplan_table):

    agent = LineageCronstructor(config)

    for _, row in loadplan_table.iterrows():
        
        loadplan_id = row['i_load_plan']
        lp_step = row['i_lp_step']
        lp_step_name = row['lp_step_name']
        par_lp_step = row['par_i_lp_step']
        lp_step_type = row['lp_step_type']

        # get or create root node(loadplan id) if par_i_lp_step is null
        if pd.isnull(par_lp_step):

            agent.get_or_create_node(target_name=loadplan_id, node_type=ObjectType.LoadPlan.__str__)

        # create PA, which indicates parallel processn node
        if lp_step_type == 'PA':
            agent.get_or_create_node(target_name='PA'+'_'+{loadplan_id}, object_class=ObjectType.LoadPlan.__str__ + 'PA')


            # get the node in previous step
            
            # create relationship between current and previous
        
        # create package(scenario) node
        if lp_step_type == 'RS':
            agent.get_or_create_node(target_name=lp_step_name, object_class=ObjectType.Package.__str__)


            # get the node in previous step
            
            # create relationship between current and previous


    return print('done')



def get_etl_info(config, qm, loadplan_id):
    sql_agent = OracleAgent(config=config['ODI'])
    lp_query = qm.get_loadplan_step_test

    loadplan_table = sql_agent.read_table(query=lp_query.format(loadplan_id=loadplan_id))

    # package dict
    scen_list = []
    for _, row in loadplan_table[loadplan_table['lp_step_type'] == 'RS'].iterrows():
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

    return loadplan_table, df_scenario_steps


def create_odi_lineage(config, qm, loadplan_id):

    loadplan_table, df_scenario_steps = get_etl_info(config, qm, loadplan_id)

    # create loadplan lineages
    create_loadplan_lineage(config, loadplan_table)

    # create sceanrio lineages


    return 