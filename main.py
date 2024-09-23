import pandas as pd
import argparse
from src.utils import read_config
from src.enum import DBEnum, LineageType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from models.queries import bidb_test_query



def get_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument('--DB')
    # parser.add_grguemnt('--JobType')
    # parser.add_arguemnt('--LLM')

    return parser.parse_args()


if __name__ == '__main__':
    configs = read_config(".env/info.json")
    args = get_argument()
    llm_type = LlmType.AOAI

    sql_deconstructor = SQLDeconstructor(configs, llm_type)

    if args.DB.upper() == DBEnum.BIDB:
        query = bidb_test_query
            
        test_case = ['C$_0W_YFY_AV_TW_R',
                'C$_0W_YFY_IND_FIN_INFO_FS',
                'OP_FACT_CHP_INVENTORY_ETH_PULP',
                'OP_FACT_CHP_INVENTORY_REDEFINE',
                'OP_FACT_CHP_SALES_DETAILS']
        
        db_name = 'BIDB'

        type = LineageType.DataSourceOnly

        desconstructed_sql = sql_deconstructor.run(db_name, query, test_case, type)

        # desconstructed_sql.to_csv('./result/auto_result1.csv')

        print('done')

    elif args.DB.upper() == DBEnum.DWDB:
        db_name = 'DWDB'
        # sql_deconstructor.run(db_name, query, test_case)