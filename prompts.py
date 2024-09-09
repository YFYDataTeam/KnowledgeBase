SYSTEM_PROMPT="""
   I will provide the table_name and the corresponing Oracle SQL. 
    Recongize all the unique identifer such as DIM_ORG org, the unique identifier is org.
    Understand the relationship, such as org.org_code which means the column org_code in DIM_ORG.
    Don't include the 'SELECT' part since it's not relationship.
    Return the answer with the ORIGINAL table name instead of the unique identifier.

    Following is two example, give me the answer based on real case:
    Example 1
        ### Oracle SQL:
            from WBIPD_PRODUCTION_INDS_MF p,
            (select 
                    org_code,
                    check_date,
                    EMISSION_CATEGORY,
                    sum(co2_qty) co2_qty
                from WACES_CARBON_EMS_DF
                where 1=1
                and checktype_name = '月盤查'
                group by org_code, check_date,EMISSION_CATEGORY) c,
            DIM_ORG org
            where 1=1
            and c.org_code = p.org_code(+)
            and org.org_code = c.org_code
            and org.rpt_used = 'ESG碳排分析'

        ### Result, use \n\n after the end of datasource part:
            Datasource = ['WACES_CARBON_EMS_DF', 'WBIPD_PRODUCTION_INDS_MF', 'DIM_ORG']
            \n\n
            Relationship = 
            [
            (GROUPBY: WACES_CARBON_EMS_DF filter with checktype_name = '月盤查', group by org_code, check_date,EMISSION_CATEGORY),
            (JOIN: WACES_CARBON_EMS_DF.org_code = WBIPD_PRODUCTION_INDS_MF.org_code(+)),
            (JOIN: DIM_ORG.org_code = WACES_CARBON_EMS_DF.org_code),
            (Filter: DIM_ORG.rpt_used = 'ESG碳排分析'),
            ]
        
    Example 2
        ### Oracle SQL:
            FROM W_FACTORY_INV_BALANCE_F
            UNION ALL
            SELECT PERIOD_NAME,STOCK_DATE TDATE,ORG_CODE,'紙漿' STOCK_TYPE, 4 STOCK_TYPE_SORT
            ,INVENTORY_ITEM_ID
            ,UOM,QTY
            FROM W_FACTORY_INV_F INV
                ,(select max(stock_date) tdate from W_FACTORY_INV_F where ORG_CODE='ETH' AND MEMO='花蓮自製漿' AND PERIOD_NAME = to_char(CURRENT_DATE,'yyyy/mm') ) MS
            WHERE 1=1
            and INV.STOCK_DATE = MS.TDATE
            AND INV.ORG_CODE='ETH' AND INV.MEMO='花蓮自製漿'"

        ### Result, use \n\n after the end of datasource part:
            Datasource = ['W_FACTORY_INV_BALANCE_F', 'W_FACTORY_INV_F']
            \n\n
            Relationship = 
            [
            (Filter: W_FACTORY_INV_F.ORG_CODE = 'ETH'),
            (Filter: W_FACTORY_INV_F.MEMO = '花蓮自製漿'),
            (JOIN: W_FACTORY_INV_F.STOCK_DATE = (SELECT max(stock_date) FROM W_FACTORY_INV_F WHERE ORG_CODE='ETH' AND MEMO='花蓮自製漿' AND PERIOD_NAME = to_char(CURRENT_DATE, 'yyyy/mm')) MS.TDATE)
            ]

    Just export the summary without any other description.
    
    table_name: {table_name}

    datasource: {datasource}

"""

SYSTEM_PROMPT_v2="""
   I will provide the table_name and the corresponing Oracle SQL. 
    Recongize all the unique identifer such as DIM_ORG org, the unique identifier is org.
    Understand the relationship, such as org.org_code which means the column org_code in DIM_ORG.
    Return the answer with the ORIGINAL table name instead of the unique identifier.

    Following is the example, give me the answer based on real case:
    ### Oracle SQL:
        FROM W_FACTORY_INV_BALANCE_F
        UNION ALL
        SELECT PERIOD_NAME,STOCK_DATE TDATE,ORG_CODE,'紙漿' STOCK_TYPE, 4 STOCK_TYPE_SORT
        ,INVENTORY_ITEM_ID
        ,UOM,QTY
        FROM W_FACTORY_INV_F INV
            ,(select max(stock_date) tdate from W_FACTORY_INV_F where ORG_CODE='ETH' AND MEMO='花蓮自製漿' AND PERIOD_NAME = to_char(CURRENT_DATE,'yyyy/mm') ) MS
        WHERE 1=1
        and INV.STOCK_DATE = MS.TDATE
        AND INV.ORG_CODE='ETH' AND INV.MEMO='花蓮自製漿'"

    ### Result, use \n\n after the end of datasource part:
        Datasource = ['W_FACTORY_INV_BALANCE_F', 'W_FACTORY_INV_F']

        \n\n

        [
        (Filter: W_FACTORY_INV_F.ORG_CODE = 'ETH'),
        (Filter: W_FACTORY_INV_F.MEMO = '花蓮自製漿'),
        (JOIN: W_FACTORY_INV_F.STOCK_DATE = (SELECT max(stock_date) FROM W_FACTORY_INV_F WHERE ORG_CODE='ETH' AND MEMO='花蓮自製漿' AND PERIOD_NAME = to_char(CURRENT_DATE, 'yyyy/mm')) MS.TDATE)
        ]


    Just export the summary without any other description.
    
    table_name: {table_name}

    datasource: {datasource}

"""