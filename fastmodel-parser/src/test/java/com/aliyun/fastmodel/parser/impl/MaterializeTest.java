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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.MaterializedType;
import com.aliyun.fastmodel.core.tree.statement.materialize.CreateMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.DropMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.RenameMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeAlias;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeComment;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeRefProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 物化的操作单元测试
 *
 * @author panguanjing
 * @date 2020/9/22
 */
public class MaterializeTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testCreateWithoutPropertyKey() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "create materialized view www alias 'alias' references (a) engine odps with('ds'='abc')"
        );
        CreateMaterialize createMaterialize = (CreateMaterialize)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(createMaterialize.getProperties().size(), 1);
        assertEquals(createMaterialize.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testCreate() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "create materialized view wh references (abc) comment 'comment' engine odps with properties"
                + "('partition'='ds')");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), CreateMaterialize.class);
        CreateMaterialize createMaterializeStatement = (CreateMaterialize)parse;
        Identifier engine = createMaterializeStatement.getEngine();
        assertEquals("odps", engine.getValue());
        assertEquals(new Comment("comment"), createMaterializeStatement.getComment());
    }

    @Test
    public void testWithBusinessUnit() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "create materialized view ut.wh references(abc) comment 'comment' engine odps with properties"
                + "('partition'='ds')");
        CreateMaterialize createMaterializeStatement = (CreateMaterialize)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createMaterializeStatement.getBusinessUnit(), "ut");
    }

    @Test
    public void testWithIndicator() {
        DomainLanguage domainLanguage = new DomainLanguage("CREATE MATERIALIZED view wq_test"
            + ".dws_wqtest_indicator_group\n"
            + "references (ind1, ind2, ind3, ind4)\n"
            + "comment '梧箐测试'\n"
            + "engine odps\n"
            + "with properties('dim'='dim_sku,dim_shop', 'life_circle'='365');");

        CreateMaterialize createMaterializeStatement = (CreateMaterialize)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(createMaterializeStatement.getBusinessUnit(), "wq_test");
    }

    @Test
    public void testAlter() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "alter materialized view wh references (abc, bcd) engine abc set properties('abc' = 'bcd')");
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), SetMaterializeRefProperties.class);
        SetMaterializeRefProperties alterMaterializeRefPropertiesStatement
            = (SetMaterializeRefProperties)parse;
        assertEquals(alterMaterializeRefPropertiesStatement.getMaterializedType(), MaterializedType.TABLE);
        assertEquals(alterMaterializeRefPropertiesStatement.getReferences().size(), 2);
        assertEquals(alterMaterializeRefPropertiesStatement.getProperties().size(), 1);
        assertEquals(alterMaterializeRefPropertiesStatement.getEngine(), new Identifier("abc"));
    }

    @Test
    public void testAlterRename() {
        DomainLanguage domainLanguage = new DomainLanguage("alter materialized view wh rename to hh");
        BaseStatement statement = fastModelAntlrParser.parse(domainLanguage);
        RenameMaterialize renameStatement = (RenameMaterialize)statement;
        assertEquals(renameStatement.getIdentifier(), "wh");
        assertEquals(renameStatement.getNewIdentifier(), "hh");
    }

    @Test
    public void testDrop() {
        DomainLanguage domainLanguage = new DomainLanguage("drop materialized view wh");
        DropMaterialize dropMaterializeStatement = (DropMaterialize)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(dropMaterializeStatement.getMaterializedType(), MaterializedType.TABLE);
        assertEquals(dropMaterializeStatement.getIdentifier(), "wh");
    }

    @Test
    public void testAlterComment() {
        DomainLanguage domainLanguage = new DomainLanguage("alter materialized view w set comment 'comment'");
        SetMaterializeComment ma = (SetMaterializeComment)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(ma.getIdentifier(), "w");
        assertEquals(ma.getComment(), new Comment("comment"));
    }

    @Test
    public void testSetProperties() {
        SetMaterializeRefProperties setMaterializeRefProperties =
            fastModelAntlrParser.parseStatement("alter materialized view wh set properties('ds'='2020010')");
        assertEquals(setMaterializeRefProperties.getProperties().size(), 1);
    }

    @Test
    public void testSetMaterializeView() {
        SetMaterializeAlias setAlias = fastModelAntlrParser.parseStatement(
            "alter materialized view wh set alias 'alias'");
        assertEquals(setAlias.getAliasedName(), new AliasedName("alias"));
    }
    
}
