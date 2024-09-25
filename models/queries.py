from enum import Enum

class Queries(Enum):
    BIDB_TEST_QUERY = """
    SELECT view_name, text FROM ALL_Views
    WHERE owner = 'ODS' AND rownum <= 5
    """
    
    ODI_TEST_CASE = """
    SELECT * FROM W_ODI_SCEN_TABLES_D
    WHERE scen_name='PKG_W_IRISO_BASE_PAPER_INV'
    """

    ERP_TO_BI_TEST_CASE = """
    select view_name, text
    from  all_views
    where view_name = 'MTL_ITEM_CATEGORIES_V';
    """