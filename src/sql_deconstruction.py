from src.utils import read_config, OracleAgent, MySQLAgent
import re, json
import pandas as pd
import numpy as np
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate
)

from src.type_enums import DBType, LineageType, LlmType
from langchain_openai import AzureChatOpenAI

import modules.prompts as prompts


class SQLDeconstructor:
    def __init__(self, configs, llm_type):
        self.configs = configs
        if llm_type == LlmType.AOAI:
            self.llm_configs = configs['AOAI']
        

    def get_db_agent(self, db_name):

        if DBType(db_name) == DBType.DWDB:
            db_agent = OracleAgent(self.configs['DW_conn_info'])
        elif DBType(db_name) == DBType.BIDB:
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

    def sql_syntax_clean(self, data):

        # data = db_agent.read_table(query=query)

        # if test_case:
        #     data = data[data['view_name'].isin(test_case)]

        # clean the original sql syntax
        data['text'] = re.sub(r'1=1.*?(\s+|$)', '', data['text'])
        data['text'] = re.sub(r'--*?(\s+|$)', '', data['text'])
        data['text'] = data['text'].replace('\n', ' ')
        data['text'] = data['text'].replace('\t', ' ')

        # data['input'] = re.sub(
        #     r"(?i)(select\b)(.*?)(\bfrom\b)",  # Match from 'SELECT' to 'FROM'
        #     r"\1 \3",                         # Keep only 'SELECT' and 'FROM'
        #     data['text'],
        #     flags=re.DOTALL                   # Make '.' match newlines
        # )
                
        data['lineage'] = ''

        return data

    def sql_deconstruction(self, data: str, llm, system_prompt):

        system_template = system_prompt
        messages = [
            SystemMessagePromptTemplate.from_template(system_template),
            HumanMessagePromptTemplate.from_template("{table_name}, {datasource}")
        ]

        CHAT_PROMPT = ChatPromptTemplate.from_messages(messages)

        chain = CHAT_PROMPT | llm

        print(f"Deconstructing '{data['view_name']}' with text length {data['text_length']}.")

        input_data = {
            'table_name': data["view_name"],
            'datasource': data["text_length"]
        }
        llm_response = chain.invoke(input_data)

        data['lineage'] = llm_response.content


        return data
    
    @staticmethod
    def string_cleaning(raw_str):
        """
        Cleans a raw string to prepare it for json.loads() by removing escape sequences,
        replacing single quotes with double quotes, and trimming extra spaces.
        
        Args:
            raw_str (str): The raw JSON-like string with potential escape characters and improper formatting.
        
        Returns:
            dict: A dictionary if the string is successfully parsed.
            str: An error message if the string cannot be parsed.
        """
        try:
            # Step 1: Remove escape sequences like \r, \n, and \t
            cleaned_str = raw_str.replace('\r', '').replace('\n', '').replace('\t', '').replace('\\','')

            # Step 2: Replace single quotes with double quotes
            cleaned_str = cleaned_str.replace("'", '"')

            cleaned_str = cleaned_str.replace("```", '')
            cleaned_str = cleaned_str.replace("json", '')

            # Step 3: Optionally, remove any extra spaces (optional but for better readability)
            cleaned_str = re.sub(r'\s+', ' ', cleaned_str)

            # Step 4: Convert the cleaned string to a Python dictionary using json.loads
            # result_dict = json.loads(cleaned_str)
            return cleaned_str
    
        except json.JSONDecodeError as e:
            # Return an error message if JSON decoding fails
            return f"Failed to parse JSON: {e}"

    def llm_result_correction(self, llm, data):

        data['llm_fixed_lineage'] = ""

        system_template = prompts.CORRECTION_PROMPT

        messages = [
            SystemMessagePromptTemplate.from_template(system_template),
            HumanMessagePromptTemplate.from_template("{input_string}")
        ]

        CHAT_PROMPT = ChatPromptTemplate.from_messages(messages)    
        chain = CHAT_PROMPT | llm

        input_data = {
            'input_string': data['lineage'],
        }
        llm_response = chain.invoke(input_data)

        data['llm_fixed_lineage'] = llm_response.content

        data['format_fixed_lineage'] = np.where(data['llm_fixed_lineage'] == 'nochange', data['lineage'], data['lineage'])
        string_cleaning_vectorized = np.vectorize(SQLDeconstructor.string_cleaning)
        data['format_fixed_lineage'] = string_cleaning_vectorized(data['format_fixed_lineage'])

        return data

    def run(self, input_data, relationship_type):

        # db_agent = self.get_db_agent(db_name)
        # input_data = self.read_data(db_agent, query, test_case)

        cleaned_input_data = self.sql_syntax_clean(input_data)

        llm = self.get_llm_agent(self.llm_configs)
        if relationship_type == LineageType.DataSourceOnly:
            system_prompt = prompts.PROMPT_ONLY_DATASOURCE

        desconstructed_sql = self.sql_deconstruction(cleaned_input_data, llm, system_prompt)

        formatted_desconstructed_sql = self.llm_result_correction(llm, desconstructed_sql)

        return formatted_desconstructed_sql