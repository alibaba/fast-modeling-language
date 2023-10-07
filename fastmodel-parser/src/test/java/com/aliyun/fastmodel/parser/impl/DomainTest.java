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

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.domain.CreateDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.DropDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.RenameDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainAlias;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainComment;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.domain.UnSetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Domain修改
 *
 * @author panguanjing
 * @date 2020/9/22
 */
public class DomainTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testParse() {
        String sql = "CREATE DOMAIN domain_name";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement result = fastModelAntlrParser.parse(domainLanguage);
        Assert.assertSame(result.getClass(), CreateDomain.class);
        CreateDomain createDomainStatement = (CreateDomain)result;
        assertEquals(createDomainStatement.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testIgnoreCase() {
        DomainLanguage domainLanguage = new DomainLanguage("create Domain domain_name");
        CreateDomain parse = (CreateDomain)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "domain_name");
        assertEquals(parse.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testCreateDomainWithDot() {
        DomainLanguage d = new DomainLanguage("create domain abc.domain_name");
        CreateDomain createDomainStatement = (CreateDomain)fastModelAntlrParser.parse(d);
        assertEquals("abc", createDomainStatement.getBusinessUnit());
        assertEquals("domain_name", createDomainStatement.getIdentifier());
    }

    @Test
    public void testAlterDomainStatement() {
        String sql = "alter domain domain_name rename to domain_name2;";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        RenameDomain alterDomainNameStatement = (RenameDomain)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(alterDomainNameStatement.getIdentifier(), "domain_name");
        assertEquals(alterDomainNameStatement.getNewIdentifier(), "domain_name2");
        assertEquals(alterDomainNameStatement.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testAlterDomainProperty() {
        String sql = "alter domain domain_name set properties('unit'='unit_name');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        SetDomainProperties domainPropertiesStatement = (SetDomainProperties)fastModelAntlrParser
            .parse(domainLanguage);
        assertEquals(domainPropertiesStatement.getIdentifier(), "domain_name");
        assertEquals(domainPropertiesStatement.getProperties().size(), 1);
    }

    @Test
    public void testAlterDomainUnsetProperties() {
        String sql = "alter domain domain_name unset properties('unit','unit_name');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        UnSetDomainProperties unSetPropertiesStatement
            = (UnSetDomainProperties)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(unSetPropertiesStatement.getPropertyKeys().size(), 2);
        assertEquals(unSetPropertiesStatement.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testAlterDomainComment() {
        String sql = "alter domain domain_name set comment 'comment'";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        SetDomainComment commentStatement = (SetDomainComment)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(commentStatement.getComment(), new Comment("comment"));
        assertEquals(commentStatement.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testAlterDomainRename() {
        String sql = "alter domain domain_name rename to u.domain_name2";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        RenameDomain alterTableNameStatement
            = (RenameDomain)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(alterTableNameStatement.getNewIdentifier(), "domain_name2");
    }

    @Test
    public void testCreateDomain() {
        String sql = "create domain ut.domain_name comment 'comment' with PROPERTIES('key1'='val1')";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateDomain createDomainStatement
            = (CreateDomain)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(createDomainStatement.getProperties().size(), 1);
        assertEquals(createDomainStatement.getIdentifier(), "domain_name");
        assertEquals(createDomainStatement.getBusinessUnit(), "ut");
        assertEquals(createDomainStatement.getStatementType(), StatementType.DOMAIN);
    }

    @Test
    public void testParseWithProperties() throws ParseException {
        String text
            = "CREATE DOMAIN domain_name COMMENT 'comment' WITH PROPERTIES ('key'='value', 'key2'='value2')";
        DomainLanguage domainLanguage = new DomainLanguage(text);
        CreateDomain result = (CreateDomain)fastModelAntlrParser.parse(domainLanguage);
        assertEquals("domain_name", result.getIdentifier());
        assertEquals(2, result.getProperties().size());
        assertEquals(new Comment("comment"), result.getComment());
    }

    @Test
    public void testDropDomain() {
        DomainLanguage domainLanguage = new DomainLanguage("drop domain domain1");
        DropDomain dropDomainStatement = (DropDomain)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(dropDomainStatement.getIdentifier(), "domain1");
    }

    @Test
    public void testToString() {
        CreateDomain createDomain = new CreateDomain(
            CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).build()
        );
        String string = createDomain.toString();
        assertEquals("CREATE DOMAIN a.b", string);
    }

    @Test
    public void testSetDomainAlias() {
        SetDomainAlias setDomainAlias = fastModelAntlrParser.parseStatement("ALTER DOMAIN a SET ALIAS 'a'");
        assertEquals(setDomainAlias.getAliasedName(), new AliasedName("a"));
    }
}
