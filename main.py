import pandas as pd
import argparse
from src.utils import read_config
from src.commontypes import DBType, LineageType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from models.queries import bidb_test_query
from tests.test_cases import BIDB_TEST_CASES


def get_argument():
    parser = argparse.ArgumentParser()
    # Enum is iterable, so we can specify the input by comprehension
    parser.add_argument('--DB', choices=[db_type.value for db_type in DBType], required=True, help="Pass the valid DB.")
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
        
        db_name = DBType.BIDB

        type = LineageType.DataSourceOnly

        desconstructed_sql = sql_deconstructor.run(db_name, query, test_case, type)

        # desconstructed_sql.to_csv('./result/auto_result1.csv')

        print('done')

    elif DBType(args.DB) == DBType.DWDB:
        db_name = DBType.DWDB
        # sql_deconstructor.run(db_name, query, test_case)

