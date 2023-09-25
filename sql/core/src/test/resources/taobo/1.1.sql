SELECT CASE
           WHEN ( if( `wl_sal_qty` = 0, 0, ( `wl_sal_amt` + `surplus_amount` + `rebate_amount` + `cost_score` ) / `wl_sal_qty` ) ) = `sale_price`
               AND ( if( `wl_sal_qty` = 0, 0, ( `wl_sal_amt` + `surplus_amount` + `rebate_amount` + `cost_score` ) / `wl_sal_qty` ) ) < `pro_prm` THEN '变价活动'
           END
FROM
    ( SELECT wl_sal_qty,
             wl_sal_amt,
             surplus_amount,
             rebate_amount,
             (-1) * cost_score cost_score,
             sale_price,
             period_date,
             pro_prm
      FROM kylin_view_ads_fact_pos_ord_analysis_wide ) table_real
WHERE CASE
          WHEN `period_date` >= '2023-07-01'
              AND `period_date` <= '2023-07-11' THEN '当前时段'
          WHEN `period_date` >= '2022-07-01'
              AND `period_date` <= '2022-07-11' THEN '对比时段'
          ELSE NULL
          END IN ('当前时段',
                  '对比时段')
GROUP BY CASE
             WHEN ( if( `wl_sal_qty` = 0, 0, ( `wl_sal_amt` + `surplus_amount` + `rebate_amount` + `cost_score` ) / `wl_sal_qty` ) ) = `sale_price`
                 AND ( if( `wl_sal_qty` = 0, 0, ( `wl_sal_amt` + `surplus_amount` + `rebate_amount` + `cost_score` ) / `wl_sal_qty` ) ) < `pro_prm` THEN '变价活动'
             END LIMIT 500