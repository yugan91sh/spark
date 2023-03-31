select
    coalesce(
            case when (
                        `t4`.`__fcol_47` = '安徽省'
                    and `t4`.`__fcol_48` = '华东'
                ) then '欧冶马鞍山'
                 else case `t4`.`__fcol_47`
                          when '陕西省' then '欧冶西安'
                          when '新疆维吾尔自治区' then '欧冶西安'
                          when '青海省' then '欧冶西安'
                          when '宁夏回族自治区' then '欧冶西安'
                          when '甘肃省' then '欧冶西安'
                          when '重庆市' then '欧冶成都'
                          when '云南省' then '欧冶成都'
                          when '西藏自治区' then '欧冶成都'
                          when '四川省' then '欧冶成都'
                          when '贵州省' then '欧冶成都'
                          when '山西省' then '欧冶太原'
                          when '内蒙古自治区' then '欧冶太原'
                          when '天津市' then '欧冶天津'
                          when '山东省' then '欧冶天津'
                          when '河北省' then '欧冶天津'
                          when '北京市' then '欧冶天津'
                          when '海南省' then '欧冶广州'
                          when '广西壮族自治区' then '欧冶广州'
                          when '广东省' then '欧冶广州'
                          when '福建省' then '欧冶广州'
                          when '香港' then '欧冶广州'
                          when '台湾' then '欧冶广州'
                          when '澳门' then '欧冶广州'
                          when '黑龙江省' then '欧冶沈阳'
                          when '吉林省' then '欧冶沈阳'
                          when '辽宁省' then '欧冶沈阳'
                          when '江西省' then '欧冶武汉'
                          when '湖南省' then '欧冶武汉'
                          when '湖北省' then '欧冶武汉'
                          when '河南省' then '欧冶武汉'
                     end
                end,
            '华东'
        ) `__fcol_53`,
    count(distinct case when (
                `t4`.`__fcol_46` = '知钢-小程序'
            and `t4`.`__fcol_44` is null
        ) then `t4`.`__fcol_45`
                        else `t4`.`__fcol_44`
        end) `__fcol_54`
from (
         select
             `t3`.`__fcol_35` `__fcol_44`,
             `t3`.`__fcol_36` `__fcol_45`,
             `t3`.`__fcol_38` `__fcol_46`,
             `t3`.`__fcol_39` `__fcol_47`,
             `t3`.`__fcol_40` `__fcol_48`,
             cast(`t3`.`__fcol_37` as date) `__fcol_50`
         from (
                  select
                      `t2`.`__fcol_25` `__fcol_35`,
                      `t2`.`__fcol_26` `__fcol_36`,
                      `t2`.`__fcol_27` `__fcol_37`,
                      `t2`.`__fcol_28` `__fcol_38`,
                      `t2`.`__fcol_29` `__fcol_39`,
                      `t2`.`__fcol_30` `__fcol_40`,
                      `t2`.`__fcol_32` `__fcol_41`,
                      coalesce(
                              case `t2`.`__fcol_32`
                                  when '188活动' then '欧冶钢好小程序'
                                  when '东鼎C区展厅体验' then '欧冶钢好小程序'
                                  when '人气店铺' then '欧冶钢好小程序'
                                  when '地区公司会议签到' then '欧冶钢好小程序'
                                  when '开团抽奖' then '欧冶钢好小程序'
                                  when '微信群抽奖' then '欧冶钢好小程序'
                                  when '欧冶钢好APP-其他' then '欧冶钢好APP'
                                  when '欧冶钢好小程序-其他' then '欧冶钢好小程序'
                                  when '直播拉新' then '欧冶钢好小程序'
                                  when '知钢小程序' then '知钢小程序'
                                  when '综合平台扫码登录' then '欧冶钢好小程序'
                                  when '裂变游戏' then '欧冶钢好小程序'
                                  when '资讯拉新' then '欧冶钢好小程序'
                                  when '邀请有奖' then '欧冶钢好APP'
                                  end,
                              `t2`.`__fcol_34`
                          ) `__fcol_43`
                  from (
                           select
                               `t1`.`__fcol_12` `__fcol_25`,
                               `t1`.`__fcol_13` `__fcol_26`,
                               `t1`.`__fcol_14` `__fcol_27`,
                               `t1`.`__fcol_15` `__fcol_28`,
                               `t1`.`__fcol_16` `__fcol_29`,
                               `t1`.`__fcol_17` `__fcol_30`,
                               coalesce(
                                       case `t1`.`__fcol_24`
                                           when trim('直播拉新            ') then trim('直播拉新         ')
                                           when trim('助力活动(7.9后)            ') then trim('188活动         ')
                                           when trim('21年1018-聚人气            ') then trim('21年1018        ')
                                           when trim('22年1018-投票              ') then trim('22年1018        ')
                                           when trim('资讯拉新                   ') then trim('资讯拉新        ')
                                           when trim('22年1018-活动专区          ') then trim('22年1018        ')
                                           when trim('22年1018-活动专区-未来可期 ') then trim('22年1018        ')
                                           when trim('22年1018-活动专区-店铺 ') then trim('22年1018        ')
                                           when trim('自然注册-店铺关注          ') then trim('自然注册        ')
                                           when trim('人气店铺                   ') then trim('人气店铺        ')
                                           when trim('22年1018-预热活动          ') then trim('22年1018        ')
                                           when trim('自然注册-管理员邀请        ') then trim('自然注册        ')
                                           when trim('裂变游戏                   ') then trim('裂变游戏        ')
                                           when trim('自然注册-加车              ') then trim('自然注册        ')
                                           when trim('自然注册-看价格            ') then trim('自然注册        ')
                                           when trim('自然注册-看质保书          ') then trim('自然注册        ')
                                           when trim('地区公司会议签到           ') then trim('地区公司会议签到')
                                           when trim('开团抽奖                   ') then trim('开团抽奖        ')
                                           when trim('东鼎C区展厅体验            ') then trim('东鼎C区展厅体验 ')
                                           when trim('微信群抽奖                 ') then trim('微信群抽奖      ')
                                           when trim('综合平台扫码登录               ') then trim('综合平台扫码登录')
                                           when trim('店铺关注有奖               ') then trim('人气店铺        ')
                                           when trim('知钢小程序                 ') then trim('知钢小程序      ')
                                           when trim('关注店铺活动（多店）-22年起') then trim('22年1018        ')
                                           when trim('活动专区-店铺-22年1018     ') then trim('22年1018        ')
                                           when trim('欧冶钢好APP        ') then trim('欧冶钢好APP-其他')
                                           when trim('欧冶钢好小程序-其他') then trim('欧冶钢好小程序-其他')
                                           when trim('邀请有奖           ') then trim('邀请有奖')
                                           end,
                                       `t1`.`__fcol_24`
                                   ) `__fcol_32`,
                               case when `t1`.`__fcol_18` in ('钢好-APP') then '欧冶钢好APP'
                                    else '欧冶钢好小程序'
                                   end `__fcol_34`
                           from (
                                    select
                                        `t0`.`__fcol_0` `__fcol_12`,
                                        `t0`.`__fcol_1` `__fcol_13`,
                                        `t0`.`__fcol_2` `__fcol_14`,
                                        `t0`.`__fcol_3` `__fcol_15`,
                                        `t0`.`__fcol_4` `__fcol_16`,
                                        `t0`.`__fcol_5` `__fcol_17`,
                                        `t0`.`__fcol_11` `__fcol_18`,
                                        case when (
                                                `t0`.`__fcol_10` is null
                                                and (
                                                            `t0`.`__fcol_11` = '钢好-直播'
                                                        or `t0`.`__fcol_11` = '钢好-直播-小程序'
                                                        or `t0`.`__fcol_8` = '10'
                                                        or `t0`.`__fcol_8` = '11'
                                                        or `t0`.`__fcol_8` = '14'
                                                    )
                                            ) then '直播拉新'
                                             else case when (
                                                     `t0`.`__fcol_10` is null
                                                     and (
                                                                 `t0`.`__fcol_8` = '0'
                                                             or (
                                                                             substr(`t0`.`__fcol_6`,1,4) = 2021
                                                                         and (
                                                                                         substr(`t0`.`__fcol_6`,7,1) = 4
                                                                                     or substr(`t0`.`__fcol_6`,7,1) = 5
                                                                                 )
                                                                         and `t0`.`__fcol_7` = 'H5'
                                                                     )
                                                         )
                                                 ) then '邀请有奖'
                                                       else case when (
                                                               `t0`.`__fcol_10` is null
                                                               and `t0`.`__fcol_11` = '钢好-知钢-小程序'
                                                           ) then '知钢小程序'
                                                                 else case when (
                                                                         `t0`.`__fcol_10` is null
                                                                         and `t0`.`__fcol_9` = '2022年1018活动专区-C'
                                                                     ) then '22年1018-活动专区'
                                                                           else case when (
                                                                                   `t0`.`__fcol_10` is null
                                                                                   and `t0`.`__fcol_9` = '综合平台登录'
                                                                               ) then '综合平台扫码登录'
                                                                                     else case when (
                                                                                             `t0`.`__fcol_10` is null
                                                                                             and `t0`.`__fcol_11` = '钢好-APP'
                                                                                         ) then '欧冶钢好APP'
                                                                                               else case when (
                                                                                                       `t0`.`__fcol_10` is null
                                                                                                       and (
                                                                                                                   `t0`.`__fcol_11` = '钢好-小程序'
                                                                                                               or `t0`.`__fcol_11` = '钢好-H5'
                                                                                                           )
                                                                                                   ) then '欧冶钢好小程序-其他'
                                                                                                         else `t0`.`__fcol_10`
                                                                                                   end
                                                                                         end
                                                                               end
                                                                     end
                                                           end
                                                 end
                                            end `__fcol_24`
                                    from (
                                             select
                                                 `T_9C4BEE`.`N代码` `__fcol_0`,
                                                 `T_9C4BEE`.`注册手机` `__fcol_1`,
                                                 `T_9C4BEE`.`注册时间` `__fcol_2`,
                                                 `T_9C4BEE`.`注册机型` `__fcol_3`,
                                                 `T_9C4BEE`.`绑定手机号归属省份` `__fcol_4`,
                                                 `T_9C4BEE`.`绑定手机号归属区域` `__fcol_5`,
                                                 `T_9C4BEE`.`钢好注册时间` `__fcol_6`,
                                                 `T_9C4BEE`.`钢好注册机型` `__fcol_7`,
                                                 `T_9C4BEE`.`注册来源活动类型` `__fcol_8`,
                                                 `T_9C4BEE`.`注册来源活动名称` `__fcol_9`,
                                                 case `T_9C4BEE`.`注册来源活动类型`
                                                     when '3' then '东鼎C区展厅体验'
                                                     when '4' then '裂变游戏'
                                                     when '42' then '裂变游戏'
                                                     when '5' then '助力活动(7.9后)'
                                                     when '12' then '微信群抽奖'
                                                     when '13' then '地区公司会议签到'
                                                     when '16' then '人气店铺'
                                                     when '17' then '开团抽奖'
                                                     when '18' then '资讯拉新'
                                                     when '19' then '21年1018-聚人气'
                                                     when '24' then '21年1018-聚人气'
                                                     when '26' then '21年1018-聚人气'
                                                     when '29' then '21年1018-聚人气'
                                                     when '33' then '21年1018-聚人气'
                                                     when '34' then '自然注册-加车'
                                                     when '35' then '自然注册-看价格'
                                                     when '36' then '自然注册-看质保书'
                                                     when '37' then '自然注册-店铺关注'
                                                     when '38' then '自然注册-管理员邀请'
                                                     when '43' then '店铺关注有奖'
                                                     when '44' then '22年1018-活动专区-店铺'
                                                     when '47' then '22年1018-活动专区'
                                                     when '48' then '22年1018-预热活动'
                                                     when '56' then '22年1018-活动专区-未来可期'
                                                     when '57' then '22年1018-投票'
                                                     when '82' then '22年1018-活动专区'
                                                     end `__fcol_10`,
                                                 case `T_9C4BEE`.`注册机型`
                                                     when '钢好-android' then '钢好-APP'
                                                     when '钢好-ios' then '钢好-APP'
                                                     when '钢好-pro' then '钢好-APP'
                                                     when '钢好-jdsc' then '钢好-APP'
                                                     when '钢好-' then '钢好-APP'
                                                     when '钢好-web' then '钢好-H5'
                                                     when '钢好-H5' then '钢好-H5'
                                                     when '钢好-live' then '钢好-直播'
                                                     when '钢好-livewxapp' then '钢好-直播-小程序'
                                                     when '钢好-livewxapp-pc' then '钢好-直播-小程序'
                                                     when '钢好-ouyeelgo' then '钢好-小程序'
                                                     when '钢好-wxapp' then '钢好-小程序'
                                                     when '钢好-windows' then '钢好-小程序'
                                                     when '钢好-ouyeelgo-pc' then '钢好-小程序'
                                                     when '知钢-小程序' then '钢好-知钢-小程序'
                                                     end `__fcol_11`
                                             from (select
                                                       ouyeel_ncode  `N代码`
                                                        , tcode  `用户T代码`
                                                        , company_code  `公司T代码`
                                                        , mobile  `注册手机`
                                                        , cast(translate(gmt_create,'/','-') as timestamp)  `创建时间`
                                                        , cast(translate(gmt_modified,'/','-') as timestamp)  `更新时间`
                                                        , is_register  `是否注册`
                                                        , cast(translate(register_time,'/','-') as timestamp)  `注册时间`
                                                        , res_from  `注册机型`
                                                        , register_source  `注册来源`
                                                        , choosechannelkey1  `推广渠道`
                                                        , state  `手机审核状态`
                                                        , audit_time  `审核时间`
                                                        , auditor  `审核人`
                                                        , is_block  `是否停用`
                                                        , cast(translate(last_login,'/','-') as timestamp)  `最近登录`
                                                        ,  login_times  `登录次数`
                                                        ,  login_days  `登录天数`
                                                        , invitationcode  `邀请码`
                                                        , province  `邀请码省份`
                                                        , business_area  `邀请码区域`
                                                        , bind_mobile  `绑定手机号`
                                                        , phone_province  `绑定手机号归属省份`
                                                        , phone_area  `绑定手机号归属区域`
                                                        , final_province  `最终省份`
                                                        , final_area  `最终区域`
                                                        , status  `公司状态`
                                                        , city  `绑定手机号归属城市`
                                                        , cast(translate(gh_register_time,'/','-') as timestamp)  `钢好注册时间`
                                                        , gh_res_from  `钢好注册机型`
                                                        , parameter3  `积点黑名单电话`
                                                        , is_accumulation_blacklist  `是否积点黑名单`
                                                        , parameter1  `积点黑名单类型`
                                                        , jd_modi_person  `积点黑名单更新人`
                                                        , cast(translate(jd_modi_date,'/','-') as timestamp)  `积点黑名单更新日期`
                                                        , remark  `积点黑名单备注`
                                                        , active_type  `注册来源活动类型`
                                                        , active_name  `注册来源活动名称`
                                                        , points_status  `积点状态`
                                                        , points_status_desc  `积点冻结状态描述`
                                                        , modi_person  `积点冻结人描述`
                                                   from f_query_oysj_app_registration_data
                                                  ) `T_9C4BEE`
                                         ) `t0`
                                ) `t1`
                       ) `t2`
              ) `t3`
         where (
                           `t3`.`__fcol_43` in ('欧冶钢好APP')
                       and `t3`.`__fcol_41` in ('欧冶钢好APP-其他')
                       and cast(`t3`.`__fcol_37` as date) in (
                                                              date '2023-01-12', date '2023-01-11', date '2023-01-03', date '2023-01-06',
                                                              date '2023-01-09', date '2023-01-04', date '2023-01-13', date '2023-01-05',
                                                              date '2023-01-10', date '2023-01-07', date '2023-01-14', date '2023-01-08',
                                                              date '2023-01-02', date '2023-01-01'
                       )
                   )
     ) `t4`
where (
                  `t4`.`__fcol_50` >= date '2023-01-01'
              and `t4`.`__fcol_50` < date '2023-01-15'
          )
group by coalesce(
                 case when (
                             `t4`.`__fcol_47` = '安徽省'
                         and `t4`.`__fcol_48` = '华东'
                     ) then '欧冶马鞍山'
                      else case `t4`.`__fcol_47`
                               when '陕西省' then '欧冶西安'
                               when '新疆维吾尔自治区' then '欧冶西安'
                               when '青海省' then '欧冶西安'
                               when '宁夏回族自治区' then '欧冶西安'
                               when '甘肃省' then '欧冶西安'
                               when '重庆市' then '欧冶成都'
                               when '云南省' then '欧冶成都'
                               when '西藏自治区' then '欧冶成都'
                               when '四川省' then '欧冶成都'
                               when '贵州省' then '欧冶成都'
                               when '山西省' then '欧冶太原'
                               when '内蒙古自治区' then '欧冶太原'
                               when '天津市' then '欧冶天津'
                               when '山东省' then '欧冶天津'
                               when '河北省' then '欧冶天津'
                               when '北京市' then '欧冶天津'
                               when '海南省' then '欧冶广州'
                               when '广西壮族自治区' then '欧冶广州'
                               when '广东省' then '欧冶广州'
                               when '福建省' then '欧冶广州'
                               when '香港' then '欧冶广州'
                               when '台湾' then '欧冶广州'
                               when '澳门' then '欧冶广州'
                               when '黑龙江省' then '欧冶沈阳'
                               when '吉林省' then '欧冶沈阳'
                               when '辽宁省' then '欧冶沈阳'
                               when '江西省' then '欧冶武汉'
                               when '湖南省' then '欧冶武汉'
                               when '湖北省' then '欧冶武汉'
                               when '河南省' then '欧冶武汉'
                          end
                     end,
                 '华东')