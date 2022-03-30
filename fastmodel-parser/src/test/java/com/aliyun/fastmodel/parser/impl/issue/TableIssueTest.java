/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.parser.impl.issue;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class TableIssueTest extends BaseTest {

    @Test
    public void testParseTable() {
        String fmlSql = "CREATE DIM TABLE IF NOT EXISTS test_bu.dim_hm_fin_mng_b2c_mapping \n"
            + "(\n"
            + "   subsidiary_code string COMMENT '子公司编码',\n"
            + "   subsidiary_name string COMMENT '子公司名称',\n"
            + "   warehouse_code string COMMENT '仓(门店)编码' WITH ('dict'='warehouse_code'),\n"
            + "   warehouse_name string COMMENT '仓(门店)名称' WITH ('dict'='warehouse_name')\n"
            + ")\n"
            + " COMMENT 'B2C管理子公司维护'\n"
            + " PARTITIONED BY (ds string COMMENT '按月分区,格式YYYYMM')\n"
            + " WITH ('business_process'='fin_default')";
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        assertNotNull(fastModelParser.parse(new DomainLanguage(fmlSql)));
    }

    @Test
    public void testParseTableWithStruct() {

        String fml = "-- 不支持修改表名\n"
            + "CREATE FACT TABLE test_jiawa_test ALIAS '测试家娃处理'\n"
            + "(\n"
            + "case_id BIGINT COMMENT 'case编号'\n"
            + "    ,case_create_time STRING COMMENT 'case创建时间'\n"
            + "    ,case_modified_time STRING COMMENT 'case修改时间'\n"
            + "    ,biz_line_id BIGINT COMMENT '业务线id'\n"
            + "    ,biz_order_code STRING COMMENT '业务单据号'\n"
            + "    ,biz_order_domain BIGINT COMMENT '业务单据域'\n"
            + "    ,category_id BIGINT COMMENT '工单类型id'\n"
            + "    ,category_name STRING COMMENT '工单类型名称'\n"
            + "    ,out_biz_type STRING COMMENT '外部工单类型id'\n"
            + "    ,creator_id STRING COMMENT '创建人id'\n"
            + "    ,creator_role BIGINT COMMENT '创建人角色id'\n"
            + "    ,creator_role_name STRING COMMENT '创建人角色名称'\n"
            + "    ,creator_domain BIGINT COMMENT '创建人账号域'\n"
            + "    ,customer_id STRING COMMENT '服务对象id'\n"
            + "    ,customer_role BIGINT COMMENT '服务对象角色id'\n"
            + "    ,customer_role_name STRING COMMENT '服务对象角色名称'\n"
            + "    ,customer_domain BIGINT COMMENT '服务对象账号域'\n"
            + "    ,source STRING COMMENT '工单来源'\n"
            + "    ,case_type BIGINT COMMENT '工单类型id'\n"
            + "    ,case_type_name STRING COMMENT '工单类型'\n"
            + "    ,channel_id BIGINT COMMENT '渠道id'\n"
            + "    ,channel_name STRING COMMENT '渠道名称'\n"
            + "    ,entry_id BIGINT COMMENT '入口id'\n"
            + "    ,case_status BIGINT COMMENT '工单状态id'\n"
            + "    ,biz_status_id BIGINT COMMENT '业务状态id'\n"
            + "    ,biz_status STRING COMMENT '业务状态'\n"
            + "    ,flow_instance_id STRING COMMENT '流程实例id'\n"
            + "    ,case_end_time STRING COMMENT '工单完结时间'\n"
            + "    ,case_memo STRING COMMENT '工单备注'\n"
            + "    ,tenant_code STRING COMMENT '租户code'\n"
            + "    ,mail_nos STRING COMMENT '运单号'\n"
            + "    ,out_id STRING COMMENT '外部单号'\n"
            + "    ,customer_name STRING COMMENT '服务对象名称'\n"
            + "    ,outer_time STRING COMMENT '外部超时时间'\n"
            + "    ,logistics_node STRING COMMENT '物流节点'\n"
            + "    ,logistics_status STRING COMMENT '物流状态'\n"
            + "    ,ext_map MAP<STRING\n"
            + "    ,STRING> COMMENT '工单扩展字段'\n"
            + "    ,task_steps BIGINT COMMENT '经过task数量'\n"
            + "    ,action_steps BIGINT COMMENT '经过action数量'\n"
            + "    ,task_array ARRAY<STRUCT<task_id:BIGINT\n"
            + "    ,task_create_time:STRING\n"
            + "    ,task_modified_time:STRING\n"
            + "    ,task_status_id:BIGINT\n"
            + "    ,task_tag:STRING\n"
            + "    ,task_assign_time:STRING\n"
            + "    ,task_expect_time:STRING\n"
            + "    ,task_end_time:STRING\n"
            + "    ,ext_fields:STRING\n"
            + "    ,dept_path:STRING\n"
            + "    ,task_dealer_info:MAP<STRING\n"
            + "    ,STRING>>> COMMENT 'task明细过程数据集'\n"
            + "    ,action_array ARRAY<STRUCT<task_id:BIGINT\n"
            + "    ,action_id:BIGINT\n"
            + "    ,action_create_time:STRING\n"
            + "    ,action_modified_time:STRING\n"
            + "    ,action_type:BIGINT\n"
            + "    ,action_type_name:STRING\n"
            + "    ,hide_type:BIGINT\n"
            + "    ,action_memo:STRING\n"
            + "    ,action_ext_fields:STRING\n"
            + "    ,action_dealer_info:MAP<STRING\n"
            + "    ,STRING>>> COMMENT 'action明细过程数据集'\n"
            + "    ,fst_action_type_name STRING COMMENT '首次action类型'\n"
            + "    ,lst_action_type_name STRING COMMENT '末次action类型'\n"
            + "    ,is_end STRING COMMENT '是否完结'\n"
            + "    ,fulllink_end_time STRING COMMENT '全链路完结时间'\n"
            + "    ,action_create_time_list ARRAY<STRING> COMMENT 'action时间列表'\n"
            + "    ,employee_dealer_list ARRAY<STRUCT<`0`:STRING\n"
            + "    ,`1`:STRING\n"
            + "    ,`2`:STRING\n"
            + "    ,`3`:STRING>> COMMENT '处理人列表信息0处理人角色1处理人姓名2处理人处理时间3处理人工号'\n"
            + "    ,fulllink_end_seconds BIGINT COMMENT '全链路完结时间s'\n"
            + "    ,is_zmkm_cs_in STRING COMMENT '是否直营客服介入'\n"
            + "    ,is_up_cs_in STRING COMMENT '是否优加介入'\n"
            + "    ,is_sp_cs_in STRING COMMENT '是否sp小二介入'\n"
            + "    ,is_cp_cs_in STRING COMMENT '是否cp介入'\n"
            + "    ,fst_zmkm_cs_id STRING COMMENT '首次直营小二介入id'\n"
            + "    ,fst_zmkm_cs_name STRING COMMENT '首次直营小二介入名称'\n"
            + "    ,fst_zmkm_cs_time STRING COMMENT '首次直营小二介入时间'\n"
            + "    ,lst_zmkm_cs_id STRING COMMENT '末次直营小二介入id'\n"
            + "    ,lst_zmkm_cs_name STRING COMMENT '末次直营小二介入名称'\n"
            + "    ,lst_zmkm_cs_time STRING COMMENT '末次直营小二介入时间'\n"
            + "    ,fst_up_cs_id STRING COMMENT '首次优加小二介入id'\n"
            + "    ,fst_up_cs_name STRING COMMENT '首次优加小二介入名称'\n"
            + "    ,fst_up_cs_time STRING COMMENT '首次优加小二介入时间'\n"
            + "    ,lst_up_cs_id STRING COMMENT '末次优加小二介入id'\n"
            + "    ,lst_up_cs_name STRING COMMENT '末次优加小二介入名称'\n"
            + "    ,lst_up_cs_time STRING COMMENT '末次优加小二介入时间'\n"
            + "    ,fst_sp_cs_id STRING COMMENT '首次sp小二介入id'\n"
            + "    ,fst_sp_cs_name STRING COMMENT '首次sp小二介入名称'\n"
            + "    ,fst_sp_cs_time STRING COMMENT '首次sp小二介入时间'\n"
            + "    ,lst_sp_cs_id STRING COMMENT '末次sp小二介入id'\n"
            + "    ,lst_sp_cs_name STRING COMMENT '末次sp小二介入名称'\n"
            + "    ,lst_sp_cs_time STRING COMMENT '末次sp小二介入时间'\n"
            + "    ,fst_cp_cs_id STRING COMMENT '首次cp介入id'\n"
            + "    ,fst_cp_cs_name STRING COMMENT '首次cp介入名称'\n"
            + "    ,fst_cp_cs_time STRING COMMENT '首次cp介入时间'\n"
            + "    ,lst_cp_cs_id STRING COMMENT '末次cp介入id'\n"
            + "    ,lst_cp_cs_name STRING COMMENT '末次cp介入名称'\n"
            + "    ,lst_cp_cs_time STRING COMMENT '末次cp介入时间'\n"
            + "    ,zmkm_set ARRAY<STRING> COMMENT '直营小二集合'\n"
            + "    ,up_set ARRAY<STRING> COMMENT '优加小二集合'\n"
            + "    ,sp_set ARRAY<STRING> COMMENT 'sp小二集合'\n"
            + "    ,cp_set ARRAY<STRING> COMMENT 'cp集合'\n"
            + "    ,lg_customer_code string comment '履行对应客户code'\n"
            + "    ,lg_customer_name string comment '履行对应客户名称'\n"
            + "    ,lg_create_time string comment '物流订单创建时间'\n"
            + "\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   ds ALIAS '业务日期, yyyymmdd' STRING COMMENT '业务日期, yyyymmdd'\n"
            + ")\n"
            + "WITH('life_cycle'='1');";

        CreateTable createTable = nodeParser.parseStatement(fml);
        assertTrue(createTable.getColumnDefines().size() > 0);
    }

    @Test(expected = SemanticException.class)
    public void testMultiPrimaryKey() {
        String fml = "CREATE ADVANCED DWS TABLE IF NOT EXISTS ads_tm_ind_growth_act_mama_d\n"
            + "(cate_id ALIAS '类目id' bigint PRIMARY KEY COMMENT '类目id' REFERENCES cate.cate_id\n"
            + ",cate_flag ALIAS '类目级别' string PRIMARY KEY COMMENT '类目级别' REFERENCES cate.cate_flag\n"
            + ",exposure_uv ALIAS '曝光uv' bigint MEASUREMENT COMMENT '曝光uv' WITH ('atomic_indicator' = 'exp_uv')\n"
            + ",click_uv ALIAS '点击uv' bigint MEASUREMENT COMMENT '点击uv' WITH ('atomic_indicator' = 'clk_uv')\n"
            + ",pay_ord_byr_cnt ALIAS '引导成交成交人数' bigint MEASUREMENT COMMENT '引导成交成交人数' WITH ('atomic_indicator' = "
            + "'pay_ord_byr_cnt')\n"
            + ",pay_ord_amt ALIAS '引导成交成交金额' double MEASUREMENT COMMENT '引导成交成交金额' WITH ('atomic_indicator' = "
            + "'pay_ord_amt')\n"
            + ",charge ALIAS '消耗' double  COMMENT '消耗' \n"
            + ", CONSTRAINT c0 TIME_PERIOD KEY REFERENCES (`1d`,`ftd`,`itd`))\n"
            + " COMMENT '妈妈渠道表现表'\n"
            + " PARTITIONED BY (ds ALIAS '日期' string  COMMENT '日期' ,act_id ALIAS '活动id' string  COMMENT '活动id' "
            + "REFERENCES act.act_id,date_type ALIAS '日期类型:1d/impound_td/formal_td' string  COMMENT "
            + "'日期类型:1d/impound_td/formal_td' REFERENCES stat_date.date_type)\n"
            + " WITH (\n"
            + "    'business_category' = 'tm.ind.byr',\n"
            + "    'data_layer' = 'ADS_TM'\n"
            + ");";
        CreateTable createTable = nodeParser.parseStatement(fml);
        assertEquals(createTable.toString(), "");
    }
}
