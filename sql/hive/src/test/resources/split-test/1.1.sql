select
    ORG_NO_LV2,
    org_nm_lv2,
    ORG_NO_LV3,
    org_nm_lv3,
    comm_efdt                                 inspolcy_efdt,
    lifeins_sale_lob_cd,
    sum(comm_epnsamt_1)                       epnsamt_1,
    sum(comm_epnsamt_2)                       epnsamt_2,
    sum(comm_epnsamt_3)                       epnsamt_3,
    sum(comm_epnsamt_1) - sum(comm_epnsamt_3) epnsamt_4,
    sum(comm_epnsamt_4)                       epnsamt_5,
    sum(comm_epnsamt_5)                       epnsamt_6,
    SUM(ins_count)                            ins_count,
    year_month_val,
    year_quar_val,
    year_val
from KE.ttt
where is_sum = '1'
group by ORG_NO_LV2,
         org_nm_lv2,
         ORG_NO_LV3,
         org_nm_lv3,
         comm_efdt,
         lifeins_sale_lob_cd,
         year_month_val,
         year_quar_val,
         year_val