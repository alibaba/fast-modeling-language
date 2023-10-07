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
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.statement.adjunct.CreateAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.DropAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.RenameAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctAlias;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctComment;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctProperties;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/11/13
 */
public class AdjunctTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testCreate() {
        String sql = "create adjunct taobao.name alias 'alias' comment 'comment' as dim_shop.shop_type='2'";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(sql));
        assertEquals(parse.getClass(), CreateAdjunct.class);
        CreateAdjunct createAdjunct = (CreateAdjunct)parse;
        assertEquals(createAdjunct.getBusinessUnit(), "taobao");
        assertEquals(createAdjunct.getComment(), new Comment("comment"));
        BaseExpression expression = createAdjunct.getExpression();
        ComparisonExpression comparisonExpression = (ComparisonExpression)expression;
        TableOrColumn tableOrColumn = (TableOrColumn)comparisonExpression.getLeft();
        assertEquals(tableOrColumn.getQualifiedName().getParts().get(0), "dim_shop");
        assertEquals(createAdjunct.getStatementType(), StatementType.ADJUNCT);
        assertEquals(createAdjunct.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testCreateAdjunct() {
        String fml = "create adjunct tab.name comment 'comment'";
        BaseStatement statement = nodeParser.parseStatement(fml);
        assertEquals(statement.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testCreateWithProperty() {
        String sql = "create adjunct taobao.name comment 'comment' with('key'='value') as dim_shop.shop_type='2'";
        CreateAdjunct createAdjunct = (CreateAdjunct)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(createAdjunct.getProperties().size(), 1);
        assertEquals(createAdjunct.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testSetComment() {
        String sql = "alter adjunct taobao.name set comment 'comment'";
        SetAdjunctComment setAdjunctComment = (SetAdjunctComment)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(setAdjunctComment.getComment(), new Comment("comment"));
        assertEquals(setAdjunctComment.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testRenameAdjunct() {
        String sql = "alter adjunct taobao.name rename to taobao.name2";
        RenameAdjunct renameAdjunct = (RenameAdjunct)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(renameAdjunct.getNewIdentifier(), "name2");
        assertEquals(renameAdjunct.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testSetProperties() {
        SetAdjunctProperties setAdjunctProperties = (SetAdjunctProperties)nodeParser.parse(new DomainLanguage(
            "alter adjunct taobao.name set properties('type'='type1')"
        ));
        assertEquals(setAdjunctProperties.getProperties().size(), 1);
        assertEquals(setAdjunctProperties.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testSetPropertiesWithout() {
        SetAdjunctProperties setAdjunctProperties = (SetAdjunctProperties)nodeParser.parse(new DomainLanguage(
            "alter adjunct taobao.name set ('type'='type1')"
        ));
        assertEquals(setAdjunctProperties.getProperties().size(), 1);
        assertEquals(setAdjunctProperties.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testDropAdjunct() {
        DropAdjunct dropAdjunct = (DropAdjunct)nodeParser.parse(new DomainLanguage("drop adjunct taobao.name"));
        assertEquals(dropAdjunct.getQualifiedName(), QualifiedName.of("taobao.name"));
        assertEquals(dropAdjunct.getStatementType(), StatementType.ADJUNCT);
    }

    @Test
    public void testSetAdjunctAlias() {
        SetAdjunctAlias setAdjunctAlias = nodeParser.parseStatement("ALTER ADJUNCT abc SET ALIAS 'a'");
        assertEquals(setAdjunctAlias.getAliasedName(), new AliasedName("a"));
    }

    @Test
    public void testCreatAdjunct88Vip() {
        CreateAdjunct createAdjunct = nodeParser.parseStatement("create adjunct 88vip comment 'comment'");
        assertEquals(createAdjunct.getCommentValue(), "comment");
    }
}
