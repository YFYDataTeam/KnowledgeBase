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

SYSTEM_PROMPT_V3="""
 I will provide the table_name and the corresponing Oracle SQL. 
    Recongize all the unique identifer such as DIM_ORG org, the unique identifier is org.
    Understand the relationship, such as org.org_code which means the column org_code in DIM_ORG.
    Return the answer with the ORIGINAL table name instead of the unique identifier.

    Following is two example, give me the answer based on real case:
Example1:
### OracleSQL
SELECT PERIOD_NAME,STOCK_DATE TDATE,ORG_CODE,'紙漿' STOCK_TYPE, 4 STOCK_TYPE_SORT
  ,INVENTORY_ITEM_ID
  ,UOM,QTY
FROM W_FACTORY_INV_BALANCE_F
UNION ALL
SELECT PERIOD_NAME,STOCK_DATE TDATE,ORG_CODE,'紙漿' STOCK_TYPE, 4 STOCK_TYPE_SORT
  ,INVENTORY_ITEM_ID
  ,UOM,QTY
FROM W_FACTORY_INV_F INV
    ,(select max(stock_date) tdate from W_FACTORY_INV_F where ORG_CODE='ETH' AND MEMO='花蓮自製漿' AND PERIOD_NAME = to_char(CURRENT_DATE,'yyyy/mm') ) MS
WHERE 1=1
  and INV.STOCK_DATE = MS.TDATE
  AND INV.ORG_CODE='ETH' 
  AND INV.MEMO='花蓮自製漿'"

### Result
{{
    "Union1": {{
        "Datasource": ['W_FACTORY_INV_BALANCE_F'],
        "Filter": [],
        "Join": [],
        "Groupby": {{}}
    }},
    
    "Union2": {{
        "Datasource": ['W_FACTORY_INV_F', 'W_FACTORY_INV_F(g)'],
        "Filter": [
            "W_FACTORY_INV_F.ORG_CODE = 'ETH'",
            "W_FACTORY_INV_F.MEMO = '花蓮自製漿'"
        ],
        "Join": [
            "W_FACTORY_INV_F.STOCK_DATE = W_FACTORY_INV_F(g).TDATE"
        ],
        "Groupby": {{
            "W_FACTORY_INV_F(g)": "(SELECT MAX(stock_date) tdate FROM W_FACTORY_INV_F WHERE ORG_CODE = 'ETH' AND MEMO = '花蓮自製漿' AND PERIOD_NAME = TO_CHAR(CURRENT_DATE, 'yyyy/mm'))"
        }}
    }}
}}

Example2: In this case, there is a final part becasue the result of union is aggregated again, if the sql is not encapsulate by a groupby then don't include the final part. 
### OracleSQL
"SELECT PERIOD_NAME,TDATE,ORG_CODE
  ,CASE ORG_CODE WHEN 'FTA' THEN '久堂' WHEN 'FTE' THEN '台東' WHEN 'ETH' THEN '花蓮' ELSE '其他' END STOCK_TYPE
  ,CASE ORG_CODE WHEN 'FTA' THEN 1 WHEN 'FTE' THEN 2 WHEN 'ETH' THEN 3 ELSE 9 END STOCK_TYPE_SORT
  ,INVENTORY_ITEM_ID--,ITEM_NO
  ,UOM,SUM(QTY) QTY
FROM (

  SELECT F.PERIOD_NAME,F.BALANCE_DATE TDATE
  ,NVL(R.ORG_CODE_NEW,O.ORG_CODE) ORG_CODE
  ,F.INVENTORY_ITEM_ID
  ,'MT' UOM,F.END_QTY / 1000 AS QTY
  FROM W_YFY_INV_BALANCE_F F
      ,w_chp_item_d I
      ,W_YFY_ORG_D O
      ,W_CHP_TS_BELONG_ORG_R R
  WHERE 1=1
    AND F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)
    AND F.ORGANIZATION_ID = I.ORGANIZATION_ID AND F.INVENTORY_ITEM_ID = I.INVENTORY_ITEM_ID
    AND F.ORGANIZATION_ID = O.ORG_ID
    AND I.ITEM_NUMBER LIKE '4%' AND I.CATEGORY_CODE='FG' AND F.UOM='KG'
    AND I.ITEM_DESC_TCH <> '漿塑盤'
    AND NOT(F.SUB_INVENTORY  IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') 
         OR F.SUB_INVENTORY LIKE '%FGS%' )  
    AND R.ORG_CODE(+) = O.ORG_CODE AND R.SUBINVENTORY_CODE(+) = F.SUB_INVENTORY       
    AND F.PERIOD_NAME >= '2023/01'
  UNION ALL

  SELECT TO_CHAR(CURRENT_DATE,'YYYY/MM') PERIOD_NAME, TO_DATE(TO_CHAR(CURRENT_DATE,'YYYY/MM') || '/01','YYYY/MM/DD' ) TDATE
  ,NVL(R.ORG_CODE_NEW,O.ORG_CODE) ORG_CODE
  ,F.INVENTORY_ITEM_ID
  ,'MT' UOM,F.END_QTY / 1000 AS QTY
  FROM W_YFY_INV_BALANCE_F F
      ,w_chp_item_d I
      ,W_YFY_ORG_D O
      ,W_CHP_TS_BELONG_ORG_R R
  WHERE 1=1
    AND F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)
    AND F.ORGANIZATION_ID = I.ORGANIZATION_ID AND F.INVENTORY_ITEM_ID = I.INVENTORY_ITEM_ID
    AND F.ORGANIZATION_ID = O.ORG_ID
    AND I.ITEM_NUMBER LIKE '4%' AND I.CATEGORY_CODE='FG' AND F.UOM='KG'
    AND I.ITEM_DESC_TCH <> '漿塑盤'
    AND NOT(F.SUB_INVENTORY  IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') 
         OR F.SUB_INVENTORY LIKE '%FGS%' )  
    AND R.ORG_CODE(+) = O.ORG_CODE AND R.SUBINVENTORY_CODE(+) = F.SUB_INVENTORY       
    AND F.PERIOD_NAME = TO_CHAR(add_months(trunc(CURRENT_DATE,'mm'),-1),'YYYY/MM')
    and to_char(CURRENT_DATE,'dd') <> '01'
  UNION ALL  
  SELECT F.PERIOD_NAME,TRUNC(F.TRANSACTION_DATE) TDATE
  ,NVL(R.ORG_CODE_NEW,O.ORG_CODE) ORG_CODE
  ,F.INVENTORY_ITEM_ID
  ,'MT' UOM,F.TRANSACTION_QTY / 1000 AS QTY
  FROM W_CHP_MMT_F F
      ,w_chp_item_d I
      ,W_YFY_ORG_D O
      ,W_CHP_TS_BELONG_ORG_R R
  WHERE 1=1
    AND F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)
    AND F.ORGANIZATION_ID = I.ORGANIZATION_ID AND F.INVENTORY_ITEM_ID = I.INVENTORY_ITEM_ID
    AND F.ORGANIZATION_ID = O.ORG_ID
    AND I.ITEM_NUMBER LIKE '4%' AND I.CATEGORY_CODE='FG' AND F.TRANSACTION_UOM='KG'
    AND I.ITEM_DESC_TCH <> '漿塑盤'
    AND NOT(F.SUB_INVENTORY  IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') 
         OR F.SUB_INVENTORY LIKE '%FGS%' )  
    AND R.ORG_CODE(+) = O.ORG_CODE AND R.SUBINVENTORY_CODE(+) = F.SUB_INVENTORY       
    AND F.PERIOD_NAME = TO_CHAR(CURRENT_DATE,'YYYY/MM') and F.TRANSACTION_DATE < trunc(CURRENT_DATE)
  )  
GROUP BY PERIOD_NAME,TDATE,ORG_CODE
  ,CASE ORG_CODE WHEN 'ETH' THEN '花蓮' WHEN 'FTA' THEN '久堂' WHEN 'FTE' THEN '台東' ELSE '其他' END
  ,INVENTORY_ITEM_ID--,ITEM_NO
  ,UOM
    
### Result:
{{
    "Union1": {{
        "Datasource": ['W_YFY_INV_BALANCE_F', 'w_chp_item_d', 'W_YFY_ORG_D', 'W_CHP_TS_BELONG_ORG_R'],
        "Filter": [
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)",
            "w_chp_item_d.ITEM_NUMBER LIKE '4%'",
            "w_chp_item_d.CATEGORY_CODE = 'FG'",
            "W_YFY_INV_BALANCE_F.UOM = 'KG'",
            "w_chp_item_d.ITEM_DESC_TCH <> '漿塑盤'",
            "NOT(W_YFY_INV_BALANCE_F.SUB_INVENTORY IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') OR W_YFY_INV_BALANCE_F.SUB_INVENTORY LIKE '%FGS%')",
            "W_YFY_INV_BALANCE_F.PERIOD_NAME >= '2023/01'"
        ],
        "Join": [
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID = w_chp_item_d.ORGANIZATION_ID",
            "W_YFY_INV_BALANCE_F.INVENTORY_ITEM_ID = w_chp_item_d.INVENTORY_ITEM_ID",
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID = W_YFY_ORG_D.ORG_ID",
            "W_CHP_TS_BELONG_ORG_R.ORG_CODE(+) = W_YFY_ORG_D.ORG_CODE",
            "W_CHP_TS_BELONG_ORG_R.SUBINVENTORY_CODE(+) = W_YFY_INV_BALANCE_F.SUB_INVENTORY",

        ],
        "Groupby": {{}}
    }},
    
    "Union2": {{
        "Datasource": ['W_YFY_INV_BALANCE_F', 'w_chp_item_d', 'W_YFY_ORG_D', 'W_CHP_TS_BELONG_ORG_R'],
        "Filter": [
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)",
            "w_chp_item_d.ITEM_NUMBER LIKE '4%'",
            "w_chp_item_d.CATEGORY_CODE = 'FG'",
            "W_YFY_INV_BALANCE_F.UOM = 'KG'",
            "w_chp_item_d.ITEM_DESC_TCH <> '漿塑盤'",
            "NOT(W_YFY_INV_BALANCE_F.SUB_INVENTORY IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') OR W_YFY_INV_BALANCE_F.SUB_INVENTORY LIKE '%FGS%')",

            "W_YFY_INV_BALANCE_F.PERIOD_NAME = TO_CHAR(add_months(trunc(CURRENT_DATE, 'mm'), -1), 'YYYY/MM')",
            "TO_CHAR(CURRENT_DATE, 'dd') <> '01'"
        ],
        "Join": [
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID = w_chp_item_d.ORGANIZATION_ID",
            "W_YFY_INV_BALANCE_F.INVENTORY_ITEM_ID = w_chp_item_d.INVENTORY_ITEM_ID",
            "W_YFY_INV_BALANCE_F.ORGANIZATION_ID = W_YFY_ORG_D.ORG_ID",
            "W_CHP_TS_BELONG_ORG_R.ORG_CODE(+) = W_YFY_ORG_D.ORG_CODE",
            "W_CHP_TS_BELONG_ORG_R.SUBINVENTORY_CODE(+) = W_YFY_INV_BALANCE_F.SUB_INVENTORY",
        ],
        "Groupby": {{}}
    }},

    "Union3": {{
        "Datasource": ['W_CHP_MMT_F', 'w_chp_item_d', 'W_YFY_ORG_D', 'W_CHP_TS_BELONG_ORG_R'],
        "Filter": [
            "W_CHP_MMT_F.ORGANIZATION_ID IN (290,291,305,286,287,288,289)",
            "w_chp_item_d.ITEM_NUMBER LIKE '4%'",
            "w_chp_item_d.CATEGORY_CODE = 'FG'",
            "W_CHP_MMT_F.TRANSACTION_UOM = 'KG'",
            "w_chp_item_d.ITEM_DESC_TCH <> '漿塑盤'",
            "NOT(W_CHP_MMT_F.SUB_INVENTORY IN ('RM','MTL','SFG','SFGW','FG-PP','FG-PP1') OR W_CHP_MMT_F.SUB_INVENTORY LIKE '%FGS%')",
            "W_CHP_MMT_F.PERIOD_NAME = TO_CHAR(CURRENT_DATE, 'YYYY/MM')",
            "W_CHP_MMT_F.TRANSACTION_DATE < TRUNC(CURRENT_DATE)",

        ],
        "Join": [
            "W_CHP_MMT_F.ORGANIZATION_ID = w_chp_item_d.ORGANIZATION_ID",
            "W_CHP_MMT_F.INVENTORY_ITEM_ID = w_chp_item_d.INVENTORY_ITEM_ID",
            "W_CHP_MMT_F.ORGANIZATION_ID = W_YFY_ORG_D.ORG_ID",
            "W_CHP_TS_BELONG_ORG_R.ORG_CODE(+) = W_YFY_ORG_D.ORG_CODE",
            "W_CHP_TS_BELONG_ORG_R.SUBINVENTORY_CODE(+) = W_CHP_MMT_F.SUB_INVENTORY",
        ],
        "Groupby": {{}}
    }},

    "Final": {{
        "Datasource": ['(All Union Tables)'],
        "Filter": [],
        "Join": [],
        "Groupby": {{
            "PERIOD_NAME",
            "TDATE",
            "ORG_CODE",
            "CASE ORG_CODE WHEN 'ETH' THEN '花蓮' WHEN 'FTA' THEN '久堂' WHEN 'FTE' THEN '台東' ELSE '其他' END",
            "INVENTORY_ITEM_ID",
            "UOM"
        }}
    }}
}}

    Just export the result without any other description.
    table_name: {table_name}

    datasource: {datasource}

"""