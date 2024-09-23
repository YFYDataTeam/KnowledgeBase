bidb_test_query="""
    SELECT view_name, text FROM ALL_Views
    where owner = 'ODS' AND rownum <= 5
    """