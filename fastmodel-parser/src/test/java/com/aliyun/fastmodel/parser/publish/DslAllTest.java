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

package com.aliyun.fastmodel.parser.publish;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.parser.NodeParser;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 用于dsl all test
 *
 * @author panguanjing
 * @date 2020/12/31
 */
public class DslAllTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testParse() throws IOException {
        InputStream resourceAsStream = DslAllTest.class.getResourceAsStream("/dsl-all.txt");
        String txt = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8);
        List<BaseStatement> statements = nodeParser.multiParse(new DomainLanguage(txt));
        assertEquals(88, statements.size());
    }

    @Test(expected = ParseException.class)
    public void testParseError() {
        String sql = "CREATE ADJUNCT test_bu.sku_type_fresh comment '生鲜类目'\n"
            + "WITH (\n"
            + " 'extend_name' = 'sku_type_fresh'\n"
            + ",'biz_caliber'='生鲜类目'\n"
            + ")\n"
            + "AS sku_type='1';\n - abc";
        nodeParser.multiParse(new DomainLanguage(sql));
    }

    @Test
    public void testError() {
        String sql = "CREATE ADJUNCT test_bu.sku_type_fresh comment '生鲜类目'\n"
            + "WITH (\n"
            + " 'extend_name' = 'sku_type_fresh'\n"
            + ",'biz_caliber'='生鲜类目'\n"
            + ")\n"
            + "AS sku_type='1';\n"
            + "CREATE INDICATOR test_bu.shop_sku_7d_pay_price_001 BIGINT\n"
            + "references pay_price\n"
            + "comment '门店&商品_近7天_生鲜门店&生鲜类目_支付金额'\n"
            + "WITH (\n"
            + "     'type' = 'DERIVATIVE',\n"
            + "     'extend_name' = 'shop_sku_7d_pay_price_001',\n"
            + "     'date_period'= 'd7',\n"
            + "     'adjunct' = 'shop_type_fresh,sku_type_fresh',\n"
            + "     'dim_biz_desc' = '门店维度,商品维度',\n"
            + "     'dim' = 'dim_shop,dim_sku',\n"
            + "     'main_table' = 'fact_pay_order',\n"
            + "     'date_field' = 'gmt_create',\n"
            + "     'date_field_format' = 'yyyy-MM-dd HH:mm:ss'\n"
            + ") AS sum(fact_pay_order.pay_price);";
        List<BaseStatement> parse = nodeParser.multiParse(new DomainLanguage(sql));
        assertEquals(2, parse.size());
    }
}
