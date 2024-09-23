from src.utils import read_config, OracleAgent, MySQLAgent
import re
import pandas as pd
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate
)

from src.enum import DBEnum, LineageType
from langchain.chat_models  import AzureChatOpenAI

import models.prompts as prompts


class SqlDeconstructor:
    def __init__(self, llm_configs):
        self.llm_configs = llm_configs
        

    def get_db_agent(self, db_name):

        if db_name.upper() == DBEnum.DWDB:
            db_agent = OracleAgent(self.configs['DW_conn_info'])
        elif db_name.upper() == DBEnum.BIDB:
            db_agent = OracleAgent(self.configs['BIDB_conn_info'])

        return db_agent
    
    def get_llm_agent(self, llm_configs):

        # for AOAi
        OPENAI_API_BASE = llm_configs['OPENAI_API_BASE']
        OPEN_AI_VERSION = llm_configs['OPEN_AI_VERSION']
        GPT_DEPLOYMENT_NAME = llm_configs['GPT_DEPLOYMENT_NAME']
        OPENAI_API_KEY = llm_configs['OPENAI_API_KEY']
        OPENAI_API_TYPE = llm_configs['OPENAI_API_TYPE']

        llm = AzureChatOpenAI(
            azure_endpoint=OPENAI_API_BASE,
            openai_api_version=OPEN_AI_VERSION,
            azure_deployment=GPT_DEPLOYMENT_NAME,
            openai_api_key=OPENAI_API_KEY,
            openai_api_type=OPENAI_API_TYPE,
        )

        return llm

    def read_data(self, db_agent, query, test_case=None):

        data = db_agent.read_table(query=query)

        if test_case:
            data = data[data['view_name'].isin(test_case)]

        # clean the original sql syntax
        data['text'] = data['text'].str.replace('\n', ' ')
        data['text'] = data['text'].str.replace('\t', ' ')
        data['input'] = data['text'].apply(lambda x:re.sub(r'1=1.*?(\s+|$)', '', x)) 
        data['lineage'] = ''

        return data

    def sql_deconstruction(self, data: pd.DataFrame, llm, system_prompt):

        system_template = system_prompt
        messages = [
            SystemMessagePromptTemplate.from_template(system_template),
            HumanMessagePromptTemplate.from_template("{table_name}, {datasource}")
        ]

        CHAT_PROMPT = ChatPromptTemplate.from_messages(messages)

        chain = CHAT_PROMPT | self.llm
        for idx, row in data.iterrows():
            input_data = {
                'table_name': row.view_name,
                'datasource': row.input
            }
            llm_response = chain.invoke(input_data)

            data.at[idx, 'lineage'] = llm_response.content


        return data

    def run(self, db_name, query, test_case, type):

        db_agent = self.get_db_agent(db_name)
        llm = self.get_llm_agent(self.llm_configs)

        if type == LineageType.DataSourceOnly:
            system_prompt = prompts.PROMPT_ONLY_DATASOURCE

        input_data = self.read_data(db_agent, query, test_case)

        desconstructed_sql = self.sql_deconstruction(input_data, llm, system_prompt)

        return desconstructed_sql