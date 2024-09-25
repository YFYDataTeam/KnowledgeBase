import pandas as pd
import argparse
from src.utils import read_config
from src.commontypes import JobType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from models.queries import Queries
from src.jobs import (
    create_bidb_views_lineage, 
    create_erp_to_bidb_data_lineage
)


def get_argument():
    parser = argparse.ArgumentParser()
    # Enum is iterable, so we can specify the input by comprehension
    parser.add_argument('--JobType', choices=[db_type.value for db_type in JobType], required=True, help="Pass the valid Job.")
    # parser.add_grguemnt('--LineageType')
    # parser.add_arguemnt('--LLM')
    # parser.add_argument('--CLEANALLNODE')

    return parser.parse_args()


if __name__ == '__main__':
    configs = read_config(".env/info.json")
    args = get_argument()
    llm_type = LlmType.AOAI

    if JobType(args.JobType) == JobType.BIVIEWS:

        create_bidb_views_lineage(configs, llm_type)

        print('Lineage of Views in BIDB is created.')

    elif JobType(args.JobType) == JobType.ERPTOBI:

       create_erp_to_bidb_data_lineage(configs)

        
