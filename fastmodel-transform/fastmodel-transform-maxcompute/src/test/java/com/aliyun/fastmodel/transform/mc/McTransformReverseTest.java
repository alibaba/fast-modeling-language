/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * reverse test
 *
 * @author panguanjing
 * @date 2021/3/2
 */
public class McTransformReverseTest {

    MaxComputeTransformer mcTransformer = new MaxComputeTransformer();

    @Test
    public void reverse() {
        String mcComputerSQL =
            "CREATE TABLE order_shop(a bigint, c string);";
        ReverseContext context =
            ReverseContext.builder().reverseTableType(
                TableDetailType.NORMAL_DIM).build();
        BaseStatement from = mcTransformer.reverse(
            new DialectNode(mcComputerSQL),
            context
        );
        assertEquals(from.getClass(), CreateDimTable.class);
        assertEquals(from.toString(), "CREATE DIM TABLE order_shop \n(\n"
            + "   a BIGINT,\n"
            + "   c STRING\n"
            + ")");
    }

    @Test
    public void testCreateWithLifecycle() {
        String mc = "create table ddd (a bigint) lifecycle 10;";
        ReverseContext reverseContext = ReverseContext.builder().build();
        BaseStatement reverse = mcTransformer.reverse(new DialectNode(mc));
        assertEquals(reverse.toString(), "CREATE DIM TABLE ddd \n"
            + "(\n"
            + "   a BIGINT\n"
            + ")\n"
            + "WITH('life_cycle'='10')");
    }

    @Test
    public void testReverseWithProperties() {
        String mcComputerSQL =
            "CREATE TABLE order_shop(a bigint, c string);";
        ReverseContext context =
            ReverseContext.builder().reverseTableType(
                TableDetailType.NORMAL_DIM).property(new Property("business_process", "bp")).build();
        BaseStatement from = mcTransformer.reverse(
            new DialectNode(mcComputerSQL),
            context
        );
        assertEquals(from.toString(), "CREATE DIM TABLE order_shop \n"
            + "(\n"
            + "   a BIGINT,\n"
            + "   c STRING\n"
            + ")\n"
            + "WITH('business_process'='bp')");

        Property uuid = new Property("uuid", "1234");
        Identifier a = new Identifier(
            "a"
        );
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(a)
            .aliasedName(new AliasedName("中文名"))
            .properties(Lists.newArrayList(uuid)).build();

        context =
            ReverseContext.builder().reverseTableType(
                    TableDetailType.NORMAL_DIM).property(new Property("business_process", "bp"))
                .columnProperty(columnDefinition).build();
        from = mcTransformer.reverse(
            new DialectNode(mcComputerSQL),
            context
        );
        assertEquals(from.toString(), "CREATE DIM TABLE order_shop \n"
            + "(\n"
            + "   a ALIAS '中文名' BIGINT WITH ('uuid'='1234'),\n"
            + "   c STRING\n"
            + ")\n"
            + "WITH('business_process'='bp')");
    }

    @Test
    public void testCreateWithComplexData() {
        String mc = "create table ddd (a array<bigint>, b map<string,string>, d struct<id:string>) lifecycle 10;";
        ReverseContext reverseContext = ReverseContext.builder().build();
        BaseStatement reverse = mcTransformer.reverse(new DialectNode(mc));
        assertEquals(reverse.toString(), "CREATE DIM TABLE ddd \n"
            + "(\n"
            + "   a ARRAY<BIGINT>,\n"
            + "   b MAP<STRING,STRING>,\n"
            + "   d STRUCT<id:STRING>\n"
            + ")\n"
            + "WITH('life_cycle'='10')");
    }
}
