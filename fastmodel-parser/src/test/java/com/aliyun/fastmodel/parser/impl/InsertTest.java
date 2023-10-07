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

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/3
 */
public class InsertTest {
    NodeParser fastModelAntlrParser = new NodeParser();

    private DomainLanguage setDomainLanguage(String sql) {
        return new DomainLanguage(sql);
    }

    @Test
    public void testInsertValues() {
        String sql = "insert into tb1 (col1, col2, col3) values ('a', 'b', 10), ('c','d', 5)";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        Insert insertStatement = (Insert)parse;
        List<Identifier> columns = insertStatement.getColumns();
        assertEquals(columns.size(), 3);
        Values values = (Values)insertStatement.getQuery().getQueryBody();
        assertNotNull(values);
        assertEquals(insertStatement.getStatementType(), StatementType.INSERT);
    }

    @Test
    public void testInsertAs() {
        String sql = "insert into tb1 select a, b, c from d where e = 1  and f = 2;";
        Insert insert = fastModelAntlrParser.parseStatement(sql);
        Query query = insert.getQuery();
        assertEquals(FastModelFormatter.formatNode(query), "SELECT\n"
            + "  a\n"
            + ", b\n"
            + ", c\n"
            + "FROM\n"
            + "  d\n"
            + "WHERE e = 1 AND f = 2\n");
    }

    @Test
    public void insertWithConvert() {
        String sql = "insert into tbl (`code`, name, description) values('c','c','d');";
        Insert insert = fastModelAntlrParser.parseStatement(sql);
        List<Identifier> columns = insert.getColumns();
        assertEquals(columns.get(0), new Identifier("code", true));
    }
}


