import os
import logging
import argparse
from modules.queries import Queries
from src.utils import read_config
from src.type_enums import TestJobType, JobType, LlmType
from src.testjobs import JobDispatcher

from src.biview_lineage import create_bidb_views_lineage

def get_argument():
    parser = argparse.ArgumentParser()
    # Enum is iterable, so we can specify the input by comprehension
    parser.add_argument('--TestJobType', choices=[goal.value for goal in TestJobType], required=False, default=None, help="Pass the valid TestJob.")
    parser.add_argument('--JobType', choices=[goal.value for goal in JobType], required=False, default=None, help="Pass the valid Job.")
    # parser.add_argument('--LineageType')
    # parser.add_argument('--LLM')
    # parser.add_argument('--CLEANALLNODE')

    return parser.parse_args()


def main():
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    parent_path =  os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sql_dir = os.path.join(os.path.dirname(__file__), 'sql_files')
    configs = read_config(".env/info.json")
    args = get_argument()

    if args.TestJobType:
        dispatcher = JobDispatcher(configs, sql_dir)
        dispatcher.run_job(TestJobType[args.TestJobType])

    if args.JobType.upper() == JobType.BIVIEWS.value:
        create_bidb_views_lineage(configs, query=Queries.BI_VIEWS.value, llm_type=LlmType.AOAI)

if __name__ == '__main__':
    main()