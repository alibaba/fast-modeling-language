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

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.CreateTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.DropTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.RenameTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodAlias;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodComment;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author panguanjing
 * @date 2020/11/13
 */
public class TimePeriodTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testCreate() {
        String sql
            = "create time_period if not exists taobao.name comment 'comment' with ('type'='abc') as between 1 and"
            + " 2";
        CreateTimePeriod parse = (CreateTimePeriod)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(parse.getBusinessUnit(), "taobao");
        assertEquals(parse.getComment(), new Comment("comment"));
        BetweenPredicate betweenPredicate = parse.getBetweenPredicate();
        assertNotNull(betweenPredicate.getMin());
        assertNotNull(betweenPredicate.getMax());
        assertEquals(betweenPredicate.getOrigin(), "between 1 and 2");
        assertEquals(parse.getStatementType(), StatementType.TIME_PERIOD);
    }

    @Test
    public void testCreateTimePeriod() {

        String fml = "create time_period t.b comment 'comment'";
        BaseStatement statement = nodeParser.parseStatement(fml);
        assertNotNull(statement);
    }

    @Test
    public void testNpeError() {
        String fml = "CREATE TIME_PERIOD test_bu.1d comment 'abc' WITH (\"type\" = \"DAY\",\"extendName\" = \"last 1"
            + " day\");\n";
        CreateTimePeriod timePeriod = nodeParser.parseStatement(fml);
        assertNotNull(timePeriod);
    }

    @Test
    public void testCreateWithExpr() {
        String sql
            = "CREATE time_period junit.d3 COMMENT '近3天' AS BETWEEN sub_day(${bizdate}, 2) AND add_day(${bizdate}, 1)";
        CreateTimePeriod parse = (CreateTimePeriod)nodeParser.parse(new DomainLanguage(sql));
        BetweenPredicate betweenPredicate = parse.getBetweenPredicate();
        assertNotNull(betweenPredicate.getMin());
        assertNotNull(betweenPredicate.getMax());
        assertEquals(betweenPredicate.getOrigin(), "BETWEEN sub_day(${bizdate}, 2) AND add_day(${bizdate}, 1)");
        assertEquals(parse.getStatementType(), StatementType.TIME_PERIOD);
    }

    @Test
    public void testSetComment() {
        String sql = "alter time_period taobao.name set comment 'comment'";
        SetTimePeriodComment setAdjunctComment = (SetTimePeriodComment)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(setAdjunctComment.getComment(), new Comment("comment"));
        assertEquals(setAdjunctComment.getStatementType(), StatementType.TIME_PERIOD);
    }

    @Test
    public void testRename() {
        String sql = "alter time_period taobao.name rename to taobao.name2";
        RenameTimePeriod renameAdjunct = (RenameTimePeriod)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(renameAdjunct.getNewIdentifier(), "name2");
        assertEquals(renameAdjunct.getStatementType(), StatementType.TIME_PERIOD);
    }

    @Test
    public void testSetProperties() {
        SetTimePeriodProperties setTimePeriodProperties = (SetTimePeriodProperties)nodeParser.parse(new DomainLanguage(
            "alter time_period taobao.name set ('type'='type1') as between 1 and 3"
        ));
        assertEquals(setTimePeriodProperties.getProperties().size(), 1);
        assertNotNull(setTimePeriodProperties.getBetweenPredicate());
        assertEquals(setTimePeriodProperties.getStatementType(), StatementType.TIME_PERIOD);
    }

    @Test
    public void testSetPropertiesWithExpression() {
        SetTimePeriodProperties setTimePeriodProperties = (SetTimePeriodProperties)nodeParser.parse(new DomainLanguage(
            "ALTER time_period junit.d3 SET BETWEEN sub_day(${bizdate}, 2) AND add_day(${bizdate}, 1)"
        ));
        assertNotNull(setTimePeriodProperties.getBetweenPredicate().getMin());
        assertNotNull(setTimePeriodProperties.getBetweenPredicate().getMax());
    }

    @Test
    public void testDrop() {
        DropTimePeriod timePeriod = (DropTimePeriod)nodeParser.parse(
            new DomainLanguage("drop time_period taobao.name"));
        assertNotNull(timePeriod.getQualifiedName());
    }

    @Test
    public void testCreateTimePeriodWithoutExpression() {
        String fml = "create time_period a.b comment 'comment'";
        CreateTimePeriod createTimePeriod = nodeParser.parseStatement(fml);
        assertEquals(createTimePeriod.getComment(), new Comment("comment"));
    }

    @Test
    public void testSetPropertiesWithNoAs() {
        SetTimePeriodProperties setTimePeriodProperties = nodeParser.parseStatement(
            "alter time_period taobao.name set('abc'='type1')"
        );
        assertNull(setTimePeriodProperties.getBetweenPredicate());
    }

    @Test
    public void testSetAlias() {
        String fml = "ALTER TIME_PERIOD b SET ALIAS 'alias'";
        SetTimePeriodAlias setTimePeriodAlias = nodeParser.parseStatement(fml);
        assertEquals(setTimePeriodAlias.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testNumberStart() {
        String fml = "CREATE TIME_PERIOD 1d COMMENT '最近一天'";
        CreateTimePeriod createTimePeriod = nodeParser.parseStatement(fml);
        assertEquals(createTimePeriod.getIdentifier(), "1d");

        String[] createTime = new String[] {
            "1d", "3h", "5m", "6m", "15m"
        };

        for (String c : createTime) {
            fml = "CREATE TIME_PERIOD %s COMMENT '最近一天'";
            String fml1 = String.format(fml, c);
            CreateTimePeriod createTimePeriod1 = nodeParser.parseStatement(fml1);
            assertEquals(createTimePeriod1.getIdentifier(), c);
        }
    }

    @Test
    public void testTimePeriod() {
        CreateTimePeriod createTimePeriod = new CreateTimePeriod(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("1"))
                .comment(new Comment("abc"))
                .build(),
            null
        );
        String expected = createTimePeriod.toString();
        assertEquals(expected, "CREATE TIME_PERIOD `1` COMMENT 'abc'");
        CreateTimePeriod baseStatement = nodeParser.parseStatement(expected);
        assertEquals(baseStatement.getIdentifier(), "1");
    }

    @Test
    public void testDot() {
        String fml = "CREATE TIME_PERIOD wq_test.1d COMMENT '最近1天' WITH('type'='DAY', 'description'= '最近1天', "
            + "'extend_name'='1d', 'owner'='065709#system', 'creator'='065709#system');";
        CreateTimePeriod baseStatement = nodeParser.parseStatement(fml);
        assertEquals(baseStatement.getIdentifier(), "1d");

    }
}
