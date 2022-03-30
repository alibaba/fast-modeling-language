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

package com.aliyun.fastmodel.parser.statement;

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.parser.NodeParser;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Insert Test
 *
 * @author panguanjing
 * @date 2020/11/20
 */
public class InsertTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testInsert() {
        String sql = "insert into u.t1(a, b, c) values('a', 1, 2)";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(sql));
        Insert insert = (Insert)parse;
        assertEquals(insert.getQualifiedName(), QualifiedName.of("u.t1"));
        Query query = insert.getQuery();
        BaseQueryBody queryBody = query.getQueryBody();
        Values values = (Values)queryBody;
        List<BaseExpression> rows = values.getRows();
        Row row = (Row)rows.get(0);
        assertEquals(row.getItems().size(), 3);
        assertEquals(row.getItems().get(0), new StringLiteral("a"));
    }

    @Test
    public void testInsertToString() {
        Query a = QueryUtil.query(new Values(ImmutableList.of(
            new Row(
                ImmutableList.of(new StringLiteral("a"),
                    new LongLiteral("1"),
                    new LongLiteral("2"))
            ))));
        Insert insert = new Insert(
            QualifiedName.of("u.t1"),
            a,
            ImmutableList.of(new Identifier("a"), new Identifier("b"), new Identifier("c"))
        );
        assertEquals(insert.getColumns().get(0), new Identifier("a"));
        assertTrue(insert.toString().contains("a"));
    }
}
