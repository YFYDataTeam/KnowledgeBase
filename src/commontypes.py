from enum import Enum

class JobType(Enum):
    BIVIEWS = 'BIVIEWS'
    ERPTOBI = 'ERPTOBI'

class SourceType(Enum):
    DWDB = 'DWDB'
    BIDB = 'BIDB'
    
class LineageType(Enum):
    DataSourceOnly='DATASOURCEONLY'
    FullRelationship='FULLREL'

class LlmType(Enum):
    AOAI='AOAI'


class TableType(Enum):
    BI = {'W'}
    ERP = {'MTL', 'XXTPO'}

    @staticmethod
    def get_table_type(table_name):
        for table_type in TableType:
            if any(table_name.startswith(prefix) for prefix in table_type.value):
                return table_type
        
        return None