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
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.statement.businessunit.CreateBusinessUnit;
import com.aliyun.fastmodel.core.tree.statement.businessunit.SetBusinessUnitAlias;
import com.aliyun.fastmodel.core.tree.statement.businessunit.SetBusinessUnitComment;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/9/28
 */
public class BusinessUnitTest {
    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testCreateBu() {
        DomainLanguage d = new DomainLanguage("create business_unit unit_name comment 'comment'");
        CreateBusinessUnit createDomainStatement = (CreateBusinessUnit)fastModelAntlrParser.parse(d);
        assertEquals("unit_name", createDomainStatement.getIdentifier());
        assertEquals(createDomainStatement.getComment(), new Comment("comment"));
    }

    @Test
    public void testCreateWithoutPropertyKey() {
        String sql = "create business_unit unit_name with ('a'='b')";
        CreateBusinessUnit businessUnit = (CreateBusinessUnit)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertEquals(businessUnit.getProperties().size(), 1);
    }

    @Test
    public void testCreateBuFull() {
        DomainLanguage domainLanguage = new DomainLanguage("create business_unit new_unit comment 'comment'");
        CreateBusinessUnit createBuStatement = (CreateBusinessUnit)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(createBuStatement.getComment(), new Comment("comment"));
        assertEquals(createBuStatement.getIdentifier(), "new_unit");
    }

    @Test
    public void testAlterUnit() {
        DomainLanguage domainLanguage = new DomainLanguage("alter business_unit unit_name set comment 'new_comment'");
        SetBusinessUnitComment commentStatement = (SetBusinessUnitComment)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(commentStatement.getComment(), new Comment("new_comment"));
    }

    @Test
    public void testAlterFullUnit() {
        DomainLanguage domainLanguage = new DomainLanguage("alter business_unit unit_name set comment 'new_comment'");
        SetBusinessUnitComment commentStatement = (SetBusinessUnitComment)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(commentStatement.getComment(), new Comment("new_comment"));
        assertEquals(commentStatement.getOrigin(), domainLanguage.getText());
    }

    @Test
    public void testSetUnitAlias() {
        SetBusinessUnitAlias setBusinessUnitAlias = fastModelAntlrParser.parseStatement(
            "ALTER business_unit ub set ALIAS 'alias'");
        assertEquals(setBusinessUnitAlias.getAliasedName(), new AliasedName("alias"));
    }
}
