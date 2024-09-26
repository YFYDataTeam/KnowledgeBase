import pandas as pd
import logging
import argparse
from src.utils import read_config
from src.commontypes import JobType, LlmType
from src.sql_deconstruction import SQLDeconstructor
from models.queries import Queries
from src.jobs import JobDispatcher


def get_argument():
    parser = argparse.ArgumentParser()
    # Enum is iterable, so we can specify the input by comprehension
    parser.add_argument('--JobType', choices=[db_type.value for db_type in JobType], required=True, help="Pass the valid Job.")
    # parser.add_grguemnt('--LineageType')
    # parser.add_arguemnt('--LLM')
    # parser.add_argument('--CLEANALLNODE')

    return parser.parse_args()


def main():
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    configs = read_config(".env/info.json")
    args = get_argument()

    dispatcher = JobDispatcher(configs)
    dispatcher.run_job(JobType[args.JobType])


if __name__ == '__main__':
    main()