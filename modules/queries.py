from enum import Enum

class Queries(Enum):
    VIEWS_IN = """
    SELECT view_name, text FROM ALL_Views
    WHERE view_name IN ({view_names})
    """
    VIEWS_EQ = """
    SELECT view_name, text FROM ALL_Views
    WHERE view_name = {view_name}
    """

    def get_query(self, view_names):
        if isinstance(view_names, (list, tuple)) and len(view_names) > 1:
            # Multiple view names: Use IN clause
            formatted_view_names = ', '.join(f"'{name}'" for name in view_names)
            return self.VIEWS_IN.value.format(view_names=formatted_view_names)
        else:
            # Single view name: Use equals clause
            view_name = view_names if isinstance(view_names, str) else view_names[0]
            return self.VIEWS_EQ.value.format(view_name=f"'{view_name}'")


    BIDB_TEST_QUERY = """
    SELECT view_name, text FROM ALL_Views
    WHERE owner = 'ODS' AND rownum <= 5
    """

    ODI_TEST_CASE = """
    SELECT * FROM W_ODI_SCEN_TABLES_D
    WHERE scen_name='PKG_W_IRISO_BASE_PAPER_INV'
    """

    ERP_TO_BI_TEST_CASE = """
    SELECT view_name, text
    FROM all_v  iews
    WHERE view_name IN ('MTL_ITEM_CATEGORIES_V', 'MTL_ONHAND_QUANTITIES', 'XXTPO_BI_INFO_V', 'XXTPO_BI_RCV_INFO_V')
    """

    GET_LOADPLAN_STEP_TEST = """
    select *
    from snp_lp_step
    where i_load_plan = {loadplan_id}
        --i_load_plan=45502 --and par_i_lp_step=676502
    order by i_lp_step, nvl(par_i_lp_step,0), step_order
    """

    GET_SCENARIO_TEST = """
    --以scen_no查詢scenario中steps
    select *
    from snp_scen_step
    where scen_no=550502 and table_name IS NOT Null
    """