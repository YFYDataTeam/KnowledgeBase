import pandas as pd
import argparse
from src.utils import read_config
from src.commontypes import DBType, LineageType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from models.queries import bidb_test_query
from tests.test_cases import BIDB_TEST_CASES


def get_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument('--DB', choices=[db_type.value for db_type in DBType], required=True, help="Choose the database type (DWDB or BIDB)")
    # parser.add_grguemnt('--LineageType')
    # parser.add_arguemnt('--LLM')

    return parser.parse_args()


if __name__ == '__main__':
    configs = read_config(".env/info.json")
    args = get_argument()
    llm_type = LlmType.AOAI

    sql_deconstructor = SQLDeconstructor(configs, llm_type)

    if DBType(args.DB) == DBType.BIDB:
        query = bidb_test_query
            
        test_case = BIDB_TEST_CASES
        
        db_name = 'BIDB'

        type = LineageType.DataSourceOnly

        desconstructed_sql = sql_deconstructor.run(db_name, query, test_case, type)

        # desconstructed_sql.to_csv('./result/auto_result1.csv')

        print('done')

    elif DBType(args.DB) == DBType.DWDB:
        db_name = 'DWDB'
        # sql_deconstructor.run(db_name, query, test_case)

