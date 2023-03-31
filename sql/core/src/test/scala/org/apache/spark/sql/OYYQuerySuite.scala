/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.resourceToString

/**
 * This test suite ensures all the Star Schema Benchmark queries can be successfully analyzed,
 * optimized and compiled without hitting the max iteration threshold.
 */
class OYYQuerySuite extends BenchmarkQueryTest {

  override def beforeAll: Unit = {
    super.beforeAll

    sql(
      """
        |create table `F_QUERY_OYSJ_APP_REGISTRATION_DATA`(`OUYEEL_NCODE` varchar(4096),
        |`TCODE` varchar(4096), `COMPANY_CODE` varchar(4096), `MOBILE` varchar(4096),
        |`GMT_CREATE` varchar(4096), `GMT_MODIFIED` varchar(4096), `IS_REGISTER` varchar(4096),
        |`REGISTER_TIME` varchar(4096), `RES_FROM` varchar(4096), `REGISTER_SOURCE` varchar(4096),
        |`CHOOSECHANNELKEY1` varchar(4096), `STATE` varchar(4096), `AUDIT_TIME` varchar(4096),
        |`AUDITOR` varchar(4096), `IS_BLOCK` varchar(4096), `LAST_LOGIN` varchar(4096),
        |`LOGIN_TIMES` int, `LOGIN_DAYS` int, `INVITATIONCODE` varchar(4096), `PROVINCE` varchar(4096),
        |`BUSINESS_AREA` varchar(4096), `BIND_MOBILE` varchar(4096), `PHONE_PROVINCE` varchar(4096),
        |`PHONE_AREA` varchar(4096), `FINAL_PROVINCE` varchar(4096), `FINAL_AREA` varchar(4096),
        |`STATUS` varchar(4096), `CITY` varchar(4096), `GH_REGISTER_TIME` varchar(4096),
        |`GH_RES_FROM` varchar(4096), `PARAMETER3` varchar(4096), `IS_ACCUMULATION_BLACKLIST` varchar(4096),
        |`PARAMETER1` varchar(4096), `JD_MODI_PERSON` varchar(4096), `JD_MODI_DATE` varchar(4096),
        |`REMARK` varchar(4096), `ACTIVE_TYPE` varchar(4096), `ACTIVE_NAME` varchar(4096),
        |`POINTS_STATUS` varchar(4096), `POINTS_STATUS_DESC` varchar(4096), `MODI_PERSON` varchar(4096))
        |USING parquet
      """.stripMargin)


    /* sql(
      """
        |create table `T_ADS_SRV_HT_PUR_CONTRACT_DETAIL_TRACE`(`SOB_CODE` varchar(4096),
        |`SOB_NAME` varchar(4096), `ORG_SHORT_NAME` varchar(4096), `SO_CODE` varchar(4096),
        |`SO_NAME` varchar(4096), `BU_CODE` varchar(4096), `BU_NAME` varchar(4096), `SG_CODE` varchar(4096),
        |`SG_NAME` varchar(4096), `PUR_BIZ_TYPE` bigint, `PUR_BIZ_TYPE_DESC` varchar(4096),
        |`PUR_BIZ_SUB_TYPE` bigint, `PUR_BIZ_SUB_TYPE_DESC` varchar(4096), `PUR_USER_CODE` varchar(4096),
        |`PUR_USER_NAME` varchar(4096), `PUR_BIZ_USER_CODE` varchar(4096), `PUR_BIZ_USER_NAME` varchar(4096),
        |`PUR_BIZ_AREA_CODE` varchar(4096), `PUR_BIZ_AREA_NAME` varchar(4096), `PUR_CONTRACT_CODE` varchar(4096),
        |`PUR_M_STATUS` bigint, `PUR_M_STATUS_DESC` varchar(4096), `PUR_CARGO_DETAIL_ID` bigint,
        |`PUR_CARGO_DETAIL_CODE` varchar(4096), `PUR_D_STATUS` bigint, `PUR_D_STATUS_DESC` varchar(4096),
        |`PUR_M_CREATED_TIME` varchar(4096), `PUR_M_CREATE_DATE` varchar(4096), `PUR_M_CREATE_MONTH` varchar(4096),
        |`PUR_M_CREATE_YEAR` varchar(4096), `PUR_EFFECTIVE_TIME` varchar(4096), `PUR_EFFECTIVE_DATE` varchar(4096),
        |`PUR_EFFECTIVE_MONTH` varchar(4096), `PUR_EFFECTIVE_YEAR` varchar(4096), `PUR_PRICE` decimal(16,4),
        |`PUR_NTI_PRICE` decimal(16,4), `PUR_INSIDE_FREIGHT_PRICE` decimal(16,4), `PUR_INSIDE_INCIDENTALS_PRICE` decimal(16,4),
        |`PCTD_TRANSPORT_PUR_SETTLE_VALID_AMOUNT` decimal(18,2), `PUR_OUTSIDE_INCIDENTALS_PRICE` decimal(16,4),
        |`DELIVERY_TYPE` bigint, `DELIVERY_TYPE_DESC` varchar(4096), `FACTORY_CONTRACT_CODE` varchar(4096), `FACTORY_RES_CODE` varchar(4096),
        |`GOODS_TYPE` bigint, `GOODS_TYPE_DESC` varchar(4096), `IS_WINTER_STORAGE` varchar(4096), `MANUFACTURER_CODE` varchar(4096),
        |`MANUFACTURER_NAME` varchar(4096), `PACK_TYPE` bigint, `DIVISION_CODE` varchar(4096),
        |`DIVISION_NAME` varchar(4096), `CATEGORY_CODE` varchar(4096), `CATEGORY_NAME` varchar(4096),
        |`PRODUCT_TYPE_CODE` varchar(4096), `PRODUCT_TYPE_NAME` varchar(4096), `PRODUCT_CODE` varchar(4096),
        |`PRODUCT_NAME` varchar(4096), `PROVIDER_CODE` varchar(4096), `PROVIDER_NAME` varchar(4096),
        |`PROVIDER_TYPE` varchar(4096), `PROVIDER_TYPE_DESC` varchar(4096), `PUR_AGRT_NO` varchar(4096),
        |`PUR_ARCHIVED_TIME` varchar(4096), `PUR_CONTRACT_SEND_ORIGIN` bigint, `PUR_CONTRACT_SEND_ORIGIN_DESC` varchar(4096),
        |`PUR_DELIVERY_ADDRESS` varchar(4096), `PUR_FIRST_PAYMENT_RATIO` decimal(9,6), `PUR_LATE_DELIVERY_DATE` varchar(4096),
        |`PUR_IS_ARCHIVED` varchar(4096), `PUR_M_REMARK` varchar(4096), `PUR_OVERDUE_ARCHIVING_STEP` varchar(4096),
        |`PUR_PAYMENT_METHOD` bigint, `PUR_PAYMENT_METHOD_DESC` varchar(4096), `PUR_PAYMENT_TYPE` bigint,
        |`PUR_PAYMENT_TYPE_DESC` varchar(4096), `PUR_PRICE_TYPE` bigint, `PUR_PRICE_TYPE_DESC` varchar(4096),
        |`QUALITY_GRADE` varchar(4096), `QUALITY_GRADE_DESC` varchar(4096), `REMARK` varchar(4096),
        |`SETTLE_METHOD` bigint, `SETTLE_METHOD_DESC` varchar(4096), `SHOP_SIGN` varchar(4096),
        |`SPEC` varchar(4096), `SPEC1` decimal(16,6), `SPEC2` decimal(16,6), `SPEC3` decimal(16,6),
        |`SPEC4` decimal(16,6), `SPEC5` decimal(16,6), `SPEC6` decimal(16,6), `SPEC7` decimal(16,6),
        |`SPEC8` decimal(16,6), `SPEC9` decimal(16,6), `SPEC10` decimal(16,6), `TECHNICAL_STANDARD` varchar(4096),
        |`PUR_UP_CREDIT_AVAILABLE` decimal(18,2), `PUR_UP_CREDIT_LINE` decimal(18,2),
        |`WEIGHT_METHOD` bigint, `WEIGHT_METHOD_DESC` varchar(4096), `BUYER_CODE` varchar(4096),
        |`BUYER_NAME` varchar(4096), `IS_DIRECTION_DESC` varchar(4096), `CNYS_DELIVERY_CYCLE` varchar(4096),
        |`CNYS_WEEKLY_DELIVERY_MARK` varchar(4096), `WAREHOUSE_CODE` varchar(4096), `WAREHOUSE_NAME` varchar(4096),
        |`PCTM_PUR_PAID_AMOUNT` decimal(18,2), `PCTM_PUR_UNPAID_AMOUNT` decimal(18,2), `DELIVERY_OVERTIME_DAYS` bigint,
        |`PCTD_EARLIEST_DELIVERY_DAYS` bigint, `PCTD_PUR_REMAIN_DELIVERY_WEIGHT` decimal(16,6), `PCTD_PUR_WEIGHT` decimal(16,6),
        |`PCTD_SALE_LOCK_WEIGHT` decimal(16,6), `PCTD_SALE_WEIGHT` decimal(16,6), `PLAN_DELIVERY_PERIOD` bigint,
        |`PCTD_FREIGHT_PUR_SETTLE_NTI_PRICE` decimal(16,4), `PCTD_FREIGHT_PUR_SETTLE_PRICE` decimal(16,4),
        |`PCTD_INCIDENTAL_PUR_SETTLE_NTI_PRICE` decimal(16,4), `PCTD_INCIDENTAL_PUR_SETTLE_PRICE` decimal(16,4),
        |`PCTD_PAY_PUR_SETTLE_NTI_AVG_PRICE` decimal(16,4), `PCTD_PAY_PUR_SETTLE_NTI_VALID_AMOUNT` decimal(18,2),
        |`PCTD_PAY_PUR_SETTLE_AVG_PRICE` decimal(16,4), `PCTD_PAY_PUR_SETTLE_ADD_WEIGHT` decimal(16,6),
        |`PCTD_PAY_FREIGHT_PUR_SETTLE_AVG_PRICE` decimal(16,4), `PCTD_PAY_PUR_SETTLE_BOOK_WEIGHT` decimal(16,6),
        |`PCTD_PAY_FREIGHT_PUR_SETTLE_NTI_AVG_PRICE` decimal(16,4), `PCTD_PAY_PUR_SETTLE_SETTLED_WEIGHT` decimal(16,6),
        |`PCTD_PAY_PUR_SETTLE_WEIGHT` decimal(16,6), `PCTD_PAY_PUR_SETTLE_VALID_AMOUNT` decimal(18,2),
        |`PCTD_COST_PUR_SETTLE_NTI_AVG_PRICE` decimal(16,4), `PCTD_COST_PUR_SETTLE_AVG_PRICE` decimal(16,4),
        |`PCTD_PUR_SETTLE_FREIGHT_WEIGHT` decimal(16,6), `PCTD_PUR_SETTLE_INCIDENTAL_WEIGHT` decimal(16,6),
        |`PCTD_PUR_SETTLE_SUPERVISE_WEIGHT` decimal(16,6), `PCTD_SUPERVISE_PUR_SETTLE_NTI_PRICE` decimal(16,4),
        |`PCTD_SUPERVISE_PUR_SETTLE_PRICE` decimal(16,4), `PCTD_SALE_SETTLE_INVOICE_NTI_VALID_AMOUNT` decimal(18,2),
        |`PCTD_SALE_SETTLE_INVOICE_VALID_AMOUNT` decimal(18,2), `PCTD_SALE_SETTLE_INVOICE_SETTLE_WEIGHT` decimal(16,6),
        |`PCTD_SALE_DISCOUNT_SETTLE_NTI_AVG_PRICE` decimal(16,4), `PCTD_INVOICE_SALE_SETTLE_NTI_AVG_PRICE` decimal(16,4),
        |`PCTD_INVOICE_SALE_SETTLE_AVG_PRICE` decimal(16,4), `PCTD_SALE_DISCOUNT_SETTLE_AVG_PRICE` decimal(16,4),
        |`PCTD_AVG_DELIVERY_DAYS` bigint, `PCTD_AVG_PUTIN_DATE` varchar(4096), `PCTD_AVG_PUTOUT_DATE` varchar(4096),
        |`PCTD_MAX_PUTIN_DATE` varchar(4096), `PCTD_PUTIN_AMOUNT` decimal(18,2), `PCTD_PUTIN_PLAN_WEIGHT` decimal(16,6),
        |`PCTD_PUTIN_WEIGHT` decimal(16,6), `PCTD_PUTOUT_WEIGHT` decimal(16,6), `PCTD_STOCK_WEIGHT` decimal(16,6), `PCTD_YDRY_NO_RETURN` decimal(16,6),
        |`PCTD_YDRY_PUTIN_WEIGHT` decimal(16,6), `PCTD_BILL_WEIGHT` decimal(16,6), `IS_DIRECTLY` varchar(4096),
        |`IS_DIRECTLY_DESC` varchar(4096), `PUR_DSTN_PLACE_CITY` varchar(4096),
        |`PUR_DSTN_PLACE_PROVINCE` varchar(4096), `PUR_FREIGHT_SETTLEMENT_METHOD` varchar(4096),
        |`PUR_FREIGHT_SETTLEMENT_METHOD_DESC` varchar(4096), `PUR_INCIDENTALS_SETTLEMENT_METHOD` varchar(4096),
        |`PUR_INCIDENTALS_SETTLEMENT_METHOD_DESC` varchar(4096), `PUR_SCH_CODE` varchar(4096),
        |`PUR_SCH_STATUS` varchar(4096), `PUR_SCH_STATUS_DESC` varchar(4096),
        |`PUR_SCH_WAREHOUSE_CODE` varchar(4096), `PUR_SCH_WAREHOUSE_NAME` varchar(4096),
        |`SALE_SERVICE_COMPLETE` varchar(4096), `AVERAGE_INVOICE_DATE` varchar(4096),
        |`AVERAGE_SETTLE_DATE` varchar(4096), `FREIGHT_INVOICE_NUM` varchar(4096),
        |`INCIDENTALS_INVOICE_NUM` varchar(4096), `AVERAGE_SALE_INVOICE_DATE` varchar(4096),
        |`PUR_SERVICE_CONTRACT_CODE` varchar(4096), `USER_RANGE` varchar(4096),
        |`USER_ATTRIBUTE` varchar(4096), `STELL_BELONG` varchar(4096),
        |`IS_STEEL` varchar(4096), `IS_PARTNER` varchar(4096), `SUPPLIER_GROUP` varchar(4096),
        |`PROVIDER_PROVINCE` varchar(4096), `USER_LEVEL` varchar(4096),
        |`IN_STELL_BELONG` varchar(4096), `BELONG_GROUP_CORP` varchar(4096),
        |`ENTITY_TYPE` varchar(4096), `BELONG_PROD_BASE` varchar(4096),
        |`BELONG_SALES_SYSTEM` varchar(4096), `MEMBERSHIP_TYPE` varchar(4096),
        |`UN_BILL_WEIGHT` decimal(16,6), `PUR_MAX_PUTOUT_DATE` varchar(4096),
        |`PUR_SUBMIT_TIME` varchar(4096), `PCTD_PUR_AMOUNT` decimal(18,2),
        |`IS_SRV_CONTRACT` bigint, `PCTD_DIRECTLY_PUR_WEIGHT` decimal(16,6),
        |`PCTD_ZHUNFA_WEIGHT` decimal(16,6), `PCTD_CHUCHANG_WEIGHT` decimal(16,6), `PUR_CREATE_CONFIRM_DATE` varchar(4096),
        |`INPUT_OUT_DAY` bigint, `INPUT_NOT_NUM` decimal(16,6), `PUR_ROUTE_OPER_CO_NAME` varchar(4096),
        |`PUR_ROUTE_CODE` varchar(4096), `PUR_ROUTE_NAME` varchar(4096), `PUR_TRANSPORT_WAY_DESC` varchar(4096),
        |`PUR_SETTLE_INVOICE_FIRST_DATE` varchar(4096), `PTCD_PAY_PUR_SETTLE_INVOICE_SETTLE_WEIGHT` decimal(16,6),`DATA_TIME` timestamp)
        |USING parquet
      """.stripMargin)

    sql(
      """
        |create table `T_ADS_SRV_JYKC_STOCK_PACK_TRACE`(`SOB_CODE` varchar(4096),
        |`SOB_NAME` varchar(4096), `BU_CODE` varchar(4096), `BU_NAME` varchar(4096),
        |`PUR_CARGO_DETAIL_ID` bigint, `PUR_CARGO_DETAIL_CODE` varchar(4096), `PUR_CONTRACT_ID` bigint,
        |`PUR_CONTRACT_CODE` varchar(4096), `PUR_CONTRACT_STATUS` bigint,
        |`PUR_CONTRACT_STATUS_DESC` varchar(4096), `PUR_CONTRACT_EFFECTIVE_DATE` varchar(4096), `SALE_CONTRACT_ID` bigint, `SALE_CONTRACT_CODE` varchar(4096), `SALE_CARGO_DETAIL_ID` bigint, `SALE_CARGO_DETAIL_CODE` varchar(4096), `PACK_ID` bigint, `PACK_CODE` varchar(4096), `PUT_TYPE` bigint, `PUT_TYPE_DESC` varchar(4096), `PUTIN_ID` bigint, `PROVIDER_CODE` varchar(4096), `PROVIDER_NAME` varchar(4096), `SUPPLIER_PROVIDER_CODE` varchar(4096), `SUPPLIER_PROVIDER_NAME` varchar(4096), `PUTIN_DATE` varchar(4096), `PACK_STOCK_AGE_DAY` decimal(16,6), `PACK_STOCK_AGE_MONTH` decimal(16,6), `WAREHOUSE_CODE` varchar(4096), `WAREHOUSE_NAME` varchar(4096), `EXTERNAL_SYSTEM_CODE` varchar(4096), `EXTERNAL_SYSTEM_NAME` varchar(4096), `EXTERNAL_WAREHOUSE_CODE` varchar(4096), `EXTERNAL_WAREHOUSE_NAME` varchar(4096), `AIC_REGIST` varchar(4096), `WH_AREA_NAME` varchar(4096), `WH_PROVINCE_NAME` varchar(4096), `WH_CITY_NAME` varchar(4096), `WAREHOUSE_ADDRESS` varchar(4096), `LOCATION` varchar(4096), `PACK_TYPE` bigint, `PACK_TYPE_DESC` varchar(4096), `PACK_STATUS` bigint, `PACK_STATUS_DESC` varchar(4096), `PACK_PACKAGE_WEIGHT` decimal(16,6), `PACK_PACKAGE_QUANTITY` decimal(16,6), `PACK_MEASURE_UNIT` varchar(4096), `CATEGORY_CODE` varchar(4096), `CATEGORY_NAME` varchar(4096), `PRODUCT_TYPE_CODE` varchar(4096), `PRODUCT_TYPE_NAME` varchar(4096), `PRODUCT_CODE` varchar(4096), `PRODUCT_NAME` varchar(4096), `SHOP_SIGN` varchar(4096), `QUALITY_GRADE` varchar(4096), `MANUFACTURER_CODE` varchar(4096), `MANUFACTURER_NAME` varchar(4096), `SPEC_DESC` varchar(4096), `SPEC1` decimal(16,3), `SPEC2` decimal(8,2), `SPEC3` decimal(8,2), `SPEC4` decimal(8,2), `SPEC5` decimal(16,3), `SPEC6` decimal(16,3), `SPEC7` decimal(8,2), `SPEC8` decimal(8,2), `SPEC9` decimal(8,2), `SPEC10` decimal(8,2), `PACK_PUTIN_WEIGHT` decimal(38,6), `PACK_PUTIN_QUANTITY` decimal(38,6), `PACK_BILL_WEIGHT` decimal(38,6), `PACK_BILL_QUANTITY` decimal(38,6), `PACK_PUTOUT_WEIGHT` decimal(38,6), `PACK_PUTOUT_QUANTITY` decimal(38,6), `PACK_SALE_LOCK_WEIGHT` decimal(38,6), `PACK_SALE_LOCK_QUANTITY` decimal(38,6), `BLOCKADE_TYPE` bigint, `BLOCKADE_TYPE_DESC` varchar(4096), `BLOCKADE_REASON1` varchar(4096), `BLOCKADE_REASON2` varchar(4096), `FACT_RES_CODE` varchar(4096), `BUYER_NAME` varchar(4096), `BUYER_CODE` varchar(4096), `TECH_STANDARD` varchar(4096), `PUR_PRICE` decimal(16,4), `RESOURCE_TYPE` bigint, `RESOURCE_TYPE_DESC` varchar(4096), `PUIN_TYPE` bigint, `PUIN_TYPE_DESC` varchar(4096), `PUTOUT_DATE` varchar(4096), `DELIVERY_DAY` varchar(4096), `LATE_DELIVERY_DATE` varchar(4096), `PUR_UP_PAYMENT_METHOD` bigint, `SALE_EFFECTIVE_DATE` varchar(4096), `IS_DIRECTLY` bigint, `IS_DIRECTLY_DESC` varchar(4096), `STORE_TYPE` bigint, `STORE_TYPE_DESC` varchar(4096), `LAST_PAYMENT_DATE2` varchar(4096), `LAST_PAYMENT_DATE1` varchar(4096), `PROVIDER_PROVINCE_NAME` varchar(4096), `PACK_SALE_TI_PRICE` decimal(16,6), `PACK_SALE_NTI_PRICE` decimal(16,6), `SALE_USER_NAME` varchar(4096), `SALE_USER_CODE` varchar(4096), `SALE_BIZ_USER_CODE` varchar(4096), `SALE_BIZ_USER_NAME` varchar(4096), `SALE_BIZ_AREA_CODE` varchar(4096), `SALE_BIZ_AREA_NAME` varchar(4096), `SALE_SO_CODE` varchar(4096), `SALE_SO_NAME` varchar(4096), `PUR_BIZ_AREA_CODE` varchar(4096), `PUR_BIZ_AREA_NAME` varchar(4096), `PUR_BIZ_USER_CODE` varchar(4096), `PUR_BIZ_USER_NAME` varchar(4096), `PUR_SO_CODE` varchar(4096), `PUR_SO_NAME` varchar(4096), `PACK_STOCK_OVERDUE_DAY` bigint, `PACK_STOCK_OVERDUE_MONTH` bigint, `IS_DIRECTION` bigint, `IS_DIRECTION_DESC` varchar(4096), `SCH_CODE` varchar(4096), `SCHEM_TYPE` varchar(4096), `SCHEM_TYPE_DESC` varchar(4096), `CONSIGNMENT_MODE` bigint, `CONSIGNMENT_MODE_DESC` varchar(4096), `ROUTE_OPER_CO_CODE` varchar(4096), `ROUTE_OPER_CO_NAME` varchar(4096), `PACK_BUSINESS_CYCLE` int, `PACK_STOCK_DAYS` int, `FIRST_PUTIN_DATE` varchar(4096), `PUTIN_MONTH` varchar(4096), `PUTOUT_MONTH` varchar(4096), `IS_WINTER_STORAGE_DESC` varchar(4096), `PROCESS_ORDER_CODE` varchar(4096), `PACK_UN_BILL_WEIGHT` decimal(16,6), `RISK_LEVEL` varchar(4096), `PACK_STOCK_OVERDUE_UNPAY_WEIGHT` decimal(16,6), `READYDATECHR` varchar(4096), `SUPPLIER_USER_RANGE` varchar(4096), `SCTM_CONTRACT_REALTIME_DEPOSIT_RATIO` decimal(9,6), `DEPOSIT_RATIO` decimal(9,6), `SETTLE_METHOD` bigint, `SCTM_SALE_REMAINING_AMOUNT` decimal(18,2), `SCTM_DEBT_AMT` decimal(18,2), `ADJUST_WEIGHT` decimal(16,6), `STOCK_WEIGHT` decimal(16,6), `BIZ_TYPE` bigint, `BIZ_TYPE_DESC` varchar(4096), `DEPARTURE_TIME` varchar(4096), `ARRIVAL_TIME` varchar(4096), `PUR_TRANSPORT_WAY` bigint, `PUR_TRANSPORT_WAY_DESC` varchar(4096), `STOCK_FLAG` bigint, `STOCK_FLAG_DESC` varchar(4096), `CLEAN_CARD_FLAG` bigint, `CLEAN_CARD_FLAG_DESC` varchar(4096), `SCTM_UN_ALLOCATION_AMOUNT` decimal(18,2), `SCTM_COLLECTION_RATIO` decimal(9,6), `DNUM0_ALLOCATION_AMOUNT` decimal(18,2), `DIRECT_FINANCING_BANK` bigint, `DIRECT_FINANCING_BANK_NAME` varchar(4096), `PUR_BIZ_TYPE` bigint, `PUR_BIZ_TYPE_DESC` varchar(4096), `PUR_BIZ_SUB_TYPE` bigint, `PUR_BIZ_SUB_TYPE_DESC` varchar(4096), `ORIGIN_STOCK_WEIGHT` decimal(16,6), `DATA_TIME` timestamp)
        |USING parquet
      """.stripMargin) */

  }

  val ssbQueries = Seq("1.1")

  ssbQueries.foreach { name =>
    val queryString = resourceToString(s"oyy/$name.sql",
      classLoader = Thread.currentThread.getContextClassLoader)


    test(name) {


      withSQLConf("spark.sql.codegen.wholeStage" -> "true",
        "spark.sql.codegen.fallback" -> "true",
        "spark.sql.codegen.factoryMode" -> "FALLBACK",
//        "spark.sql.codegen.maxConditionalsInFilter" -> "3000",
//        "spark.sql.optimizer.maxConditionalsInFilter" -> "3000",
        "spark.sql.subexpressionEliminationInFilter.enabled" -> "true",
        "spark.sql.constraintPropagation.enabled" -> "true",
        "spark.driver.memory" -> "128m") {

        sql(queryString).collectAsList()
      }

      /* // check the plans can be properly generated
      val plan = sql(queryString).queryExecution.executedPlan
      checkGeneratedCode(plan) */
    }

  }
}
