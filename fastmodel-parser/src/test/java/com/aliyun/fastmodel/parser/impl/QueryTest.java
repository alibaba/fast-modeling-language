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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/18
 */
public class QueryTest extends BaseTest {

    @Test
    public void testQuery() {
        String fml = "select * from abc offset 1 limit 1";
        Query parse = parse(fml, Query.class);
        QuerySpecification queryBody = (QuerySpecification)parse.getQueryBody();
        Offset offset = queryBody.getOffset();
        Limit limit = (Limit)queryBody.getLimit();
        assertEquals(offset.getRowCount(), new LongLiteral("1"));
        assertEquals(limit.getRowCount(), new LongLiteral("1"));
    }

    @Test
    public void testQuerySubQuery() {
        String fml = "Select * from (select abc from t1)t1";
        Query parse = parse(fml, Query.class);
        assertNotNull(parse);
    }

    @Test
    public void testQueryIndicator() {
        String fml = "select  _dws_all.ds ds from dws_all";
        Query statement = nodeParser.parseStatement(fml);
        assertNotNull(statement);
    }

    @Test
    public void testQueryWithMaosun() {

        String fml = "select dim_student.student_no as student_no\n"
            + " , dim_student.student_name as student_name\n"
            + " , clock_in_class_rate_0000 as clock_in_class_rate_0000\n"
            + " , need_clock_in_class_cnt_0000 as need_clock_in_class_cnt_0000\n"
            + "  where student_no = 'xxx'\n"
            + "  order by need_clock_in_class_cnt_0000 desc\n"
            + "  limit 1000";

        Query query = nodeParser.parseStatement(fml);
        assertNotNull(query);
    }

    @Test
    public void testInTableSelect() {
        String select = "select count(*) from dim_table where name in (select value from code_table)";
        Query query = nodeParser.parseStatement(select);
        String s = FastModelFormatter.formatNode(query);
        assertEquals("SELECT count(*)\n"
            + "FROM\n"
            + "  dim_table\n"
            + "WHERE name IN (SELECT value\n"
            + "FROM\n"
            + "  code_table\n"
            + ")\n", s);
    }

    @Test
    public void testDuplicateCount() {
        String select = "select count(*) from (select student_name,teacher_name from bbbb where ds='$[yyyymmdd]' "
            + "group by  student_name,teacher_name having count(*) > 1) t;";
        Query query = nodeParser.parseStatement(select);
        String s = FastModelFormatter.formatNode(query);
        System.out.println(s);
    }
}
