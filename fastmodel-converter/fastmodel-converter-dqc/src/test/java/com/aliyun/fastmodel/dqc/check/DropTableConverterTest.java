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

package com.aliyun.fastmodel.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.conveter.dqc.check.DropTableConverter;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition.ColumnBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * drop table converter
 *
 * @author panguanjing
 * @date 2021/6/23
 */
public class DropTableConverterTest {

    DropTableConverter dropTableConverter = new DropTableConverter();

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        ColumnBuilder col1 = ColumnDefinition.builder().colName(new Identifier("col1")).primary(true);
        CreateTable build = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).
            columns(
                ImmutableList.of(col1.build())
            ).build();
        CreateTable createTable = build;
        context.setBeforeStatement(createTable);
        context.setAfterStatement(createTable);
    }

    @Test
    public void testConverter() {
        DropTable source = new DropTable(
            QualifiedName.of("dim_shop")
        );
        BaseStatement convert = dropTableConverter.convert(source, context);
        AddDqcRule addRules = (AddDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = addRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
        BaseCheckElement ruleDefinition = ruleDefinitions.get(0);
        assertEquals("ALTER DQC_RULE ON TABLE dim_shop\n"
            + "ADD (\n"
            + "   CONSTRAINT `字段规则-唯一-(col1)` CHECK(DUPLICATE_COUNT(col1) = 0) NOT ENFORCED DISABLE\n"
            + ")", addRules.toString());
    }

    @Test
    public void testDropContextNull() {
        BaseStatement ab = dropTableConverter.convert(new DropTable(QualifiedName.of("ab")), new DefaultConvertContext());
        assertNull(ab);
    }

    @Test
    public void testDropWithBefore() {
        String before = "CREATE DIM TABLE IF NOT EXISTS test_test_dqc ALIAS 'test_test_dqc' \n"
            + "(\n"
            + "   a  ALIAS 'aa' STRING NOT NULL WITH ('uuid'='tb_c-a9fcae1972754407b0640acd62e1e5f7',"
            + "'code_table'='mode'),\n"
            + "   d  ALIAS 'dd' STRING WITH ('uuid'='tb_c-2711c7fb413f4abdaca21dd51c852288'),\n"
            + "   e  ALIAS 'ee' STRING NOT NULL WITH ('uuid'='tb_c-e1c0012d09b8486b83eb0d62a03bfde2'),\n"
            + "   b  ALIAS 'bb' STRING NOT NULL WITH ('uuid'='tb_c-23ae8ae279784d3093351a443387dfbc'),\n"
            + "   cc ALIAS 'cc' STRING NOT NULL WITH ('uuid'='tb_c-b56c266be7d74107a9e394df4e673f05'),\n"
            + "   CONSTRAINT PK PRIMARY KEY(cc)\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   ds ALIAS '业务日期, yyyymmdd' STRING COMMENT '业务日期, yyyymmdd' WITH "
            + "('uuid'='tb_c-686c86dd43f245b1b59e0fcf5b4b4d29')\n"
            + ")\n"
            + "WITH('data_layer'='DIM','data_domain'='domain_purchase')";
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        DefaultConvertContext context = new DefaultConvertContext();
        context.setBeforeStatement(fastModelParser.parseStatement(before));
        BaseStatement convert = dropTableConverter.convert(new DropTable(QualifiedName.of("test_test")), context);
        assertEquals(convert.toString(), "ALTER DQC_RULE ON TABLE test_test PARTITION (ds='[a-zA-Z0-9_-]*')\n"
            + "ADD (\n"
            + "   CONSTRAINT `字段规则-非空-(a)` CHECK(NULL_COUNT(a) = 0) NOT ENFORCED DISABLE,\n"
            + "   CONSTRAINT `字段规则-表中-(a)` CHECK(IN_TABLE(a, mode, code) = 0) NOT ENFORCED DISABLE,\n"
            + "   CONSTRAINT `字段规则-非空-(e)` CHECK(NULL_COUNT(e) = 0) NOT ENFORCED DISABLE,\n"
            + "   CONSTRAINT `字段规则-非空-(b)` CHECK(NULL_COUNT(b) = 0) NOT ENFORCED DISABLE,\n"
            + "   CONSTRAINT `字段规则-非空-(cc)` CHECK(NULL_COUNT(cc) = 0) NOT ENFORCED DISABLE,\n"
            + "   CONSTRAINT `字段规则-唯一-(cc)` CHECK(DUPLICATE_COUNT(cc) = 0) NOT ENFORCED DISABLE\n"
            + ")");
    }
}
