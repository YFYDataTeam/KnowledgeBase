from enum import Enum

class JobType(Enum):
    BIVIEWS = 'BIVIEWS'
    ERPTOBI = 'ERPTOBI'
    ERPVIEWS = 'ERPVIEWS'

class DBType(Enum):
    DW = 'DW'
    BI = 'BI'
    ERP = 'ERP'
    
class LineageType(Enum):
    DataSourceOnly='DATASOURCEONLY'
    FullRelationship='FULLREL'

class LlmType(Enum):
    AOAI='AOAI'


class TableType(Enum):

    # BItable = 'BItable'
    # BIview = 'BIview'
    # ERPtable = 'ERPtable'
    # ERPview = 'ERPview'

    Table = 'Table'
    View = 'View'

class DBPrefix(Enum):

    BI = {'W_'}
    ERP = {}

    @staticmethod
    def get_db_type(table_name):
        for db_type in DBPrefix:
            if db_type != DBPrefix.ERP: 
                if any(table_name.startswith(prefix) for prefix in db_type.value):
                    return db_type
            
        return DBPrefix.ERP