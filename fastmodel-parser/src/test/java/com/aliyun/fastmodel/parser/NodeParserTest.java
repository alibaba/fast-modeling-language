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

package com.aliyun.fastmodel.parser;

import java.util.List;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/22
 */
public class NodeParserTest {

    NodeParser nodeParser = new NodeParser();

    @Test(expected = ParseException.class)
    public void testMultiParse() {
        String sql = "create table a (a bigint) comment 'comment'";
        List<BaseStatement> statements = nodeParser.multiParse(new DomainLanguage(sql));
        assertNotNull(statements);
    }

    @Test
    public void testParse() {
        CreateAtomicIndicator atomicIndicator = nodeParser.parseStatement("create atomic indicator a.b bigint");
        assertNotNull(atomicIndicator);
    }

    @Test
    public void testExpression() {
        ArithmeticBinaryExpression expression = nodeParser.parseExpression("2+3");
        assertNotNull(expression);
    }

    @Test
    public void testMultiParseNoException() {
        String sql = "create dim table a (b bigint) comment 'comment'";
        List<BaseStatement> statements = nodeParser.multiParse(new DomainLanguage(sql));
        assertEquals(1, statements.size());
    }

    @Test(expected = ParseException.class)
    public void testError() {
        String sql = "ddd;";
        BaseStatement statement = nodeParser.parseStatement(sql);
        assertNotNull(statement);
    }

    @Test
    public void testExtractTableColumn() {
        String expression = "table_code.field1 is null";
        List<TableOrColumn> extract = nodeParser.extract(new DomainLanguage(expression));
        assertEquals(extract.size(), 1);
    }

    @Test
    public void testParseExpression() {
        String c =
            "create dim table a (b array<struct<task_id:bigint comment 'comment',task_create_time:string,"
                + "task_modified_time:string,"
                + "task_status_id:bigint,task_tag:string,task_assign_time:string,task_expect_time:string,"
                + "task_end_time:string,ext_fields:string,dept_path:string,task_dealer_info:map<string,string>>>);";
        CreateDimTable createDimTable = nodeParser.parseStatement(c);
        assertEquals(createDimTable.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b ARRAY<STRUCT<task_id:BIGINT COMMENT 'comment',task_create_time:STRING,task_modified_time:STRING,"
            + "task_status_id:BIGINT,task_tag:STRING,task_assign_time:STRING,task_expect_time:STRING,"
            + "task_end_time:STRING,ext_fields:STRING,dept_path:STRING,task_dealer_info:MAP<STRING,STRING>>>\n"
            + ")");
    }

}