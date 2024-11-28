from enum import Enum

class TestJobType(Enum):
    BIVIEWS = 'BIVIEWS'
    ERPTOBI = 'ERPTOBI'
    ERPVIEWS = 'ERPVIEWS'
    LOADPLAN = 'LOADPLAN'

class JobType(Enum):
    BIVIEWS = 'BIVIEWS'


class DBType(Enum):
    DW = 'DW'
    BI = 'BI'
    ERP = 'ERP'
    
class LineageType(Enum):
    DataSourceOnly='DATASOURCEONLY'
    FullRelationship='FULLREL'

class ParseType(Enum):
    RE = 'RE'
    LLM = 'LLM'

class LlmType(Enum):
    AOAI='AOAI'


class ObjectType(Enum):

    Table = 'Table'
    View = 'View'
    LoadPlan = 'LoadPlan'
    Package = 'Package'
    Scenario = 'Scenario'
    Interface = 'Interface'
    
    def __str__(self):
        return self.value

class DBPrefix(Enum):

    BI = {'W_', 'C$_0'}
    ERP = {}

    @staticmethod
    def get_db_type(table_name):
        for db_type in DBPrefix:
            if db_type != DBPrefix.ERP: 
                if any(table_name.startswith(prefix) for prefix in db_type.value):
                    return db_type
            
        return DBPrefix.ERP