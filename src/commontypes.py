from enum import Enum

class DBType(Enum):
    DWDB='DWDB'
    BIDB='BIDB'

class LineageType(Enum):
    DataSourceOnly='DATASOURCEONLY'
    FullRelationship='FULLREL'

class LlmType(Enum):
    AOAI='AOAI'