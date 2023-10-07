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
import com.aliyun.fastmodel.core.tree.statement.businessprocess.CreateBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.DropBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.RenameBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessAlias;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessComment;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessProperties;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.UnSetBusinessProcessProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * table 修改内容
 *
 * @author panguanjing
 * @date 2020/9/22
 */
public class BusinessProcessTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testCreateWithoutPropertyKey() {
        String sql = "create business_process bp_name alias 'alias' with properties ('a'='b')";
        CreateBusinessProcess createBusinessProcess = (CreateBusinessProcess)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        assertEquals(createBusinessProcess.getProperties().size(), 1);
        assertEquals(createBusinessProcess.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testProcess() {
        String sql = "CREATE business_process  1_14255.org_contact  COMMENT '企业入职' WITH ('domain'='dingtalk');";
        CreateBusinessProcess createBusinessProcess = fastModelAntlrParser.parseStatement(sql);
        assertNotNull(createBusinessProcess);
    }

    @Test
    public void testAlterBpStatementRename() {
        String sql = "alter business_process bp_name rename to bp_name2;";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getClass(), RenameBusinessProcess.class);
        RenameBusinessProcess propertiesStatement = (RenameBusinessProcess)parse;
        assertEquals(propertiesStatement.getIdentifier(), "bp_name");
        assertEquals(propertiesStatement.getNewIdentifier(), "bp_name2");
    }

    @Test
    public void testSetWithoutProperties() {
        String sql = "alter business_process t1 set ('a' = 'b')";
        SetBusinessProcessProperties setBusinessProcessProperties = (SetBusinessProcessProperties)fastModelAntlrParser
            .parse(new DomainLanguage(sql));
        assertEquals(setBusinessProcessProperties.getProperties().size(), 1);
    }

    @Test
    public void testAlterStatementSuffixUnSetProperties() {
        String sql = "alter business_process t1 unset properties('type','type2');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement st = fastModelAntlrParser.parse(domainLanguage);
        assertEquals(st.getClass(), UnSetBusinessProcessProperties.class);
        UnSetBusinessProcessProperties propertiesStatement = (UnSetBusinessProcessProperties)st;
        assertEquals(propertiesStatement.getPropertyKeys().size(), 2);
    }

    @Test
    public void testAlterStatementProperties() {
        String sql = "alter business_process t1 set properties('type'='type1');";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        SetBusinessProcessProperties bpPropertiesStatement = (SetBusinessProcessProperties)parse;
        assertEquals(bpPropertiesStatement.getIdentifier(), "t1");
        assertEquals(bpPropertiesStatement.getProperties().size(), 1);
    }

    @Test
    public void testAlterCommentStatement() {
        String sql = "alter business_process b1 set comment 'comment'";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        SetBusinessProcessComment bpCommentStatement = (SetBusinessProcessComment)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(bpCommentStatement.getComment(), new Comment("comment"));
    }

    @Test
    public void testAlterBpStatementRenameToWithIdentifier() {
        String sql = "alter business_process bp_name rename to dingtalk.bp_name1";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        BaseStatement parse = fastModelAntlrParser.parse(domainLanguage);
        RenameBusinessProcess renameBpStatement = (RenameBusinessProcess)parse;
        assertEquals(renameBpStatement.getNewIdentifier(), "bp_name1");
    }

    @Test
    public void testDropBp() {
        DomainLanguage domainLanguage = new DomainLanguage("drop business_process b1");
        DropBusinessProcess parse = (DropBusinessProcess)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "b1");
        domainLanguage = new DomainLanguage("drop business_process b1");
        parse = (DropBusinessProcess)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "b1");
    }

    @Test
    public void testSetBusinessProcess() {
        SetBusinessProcessAlias alias = fastModelAntlrParser.parseStatement(
            "alter business_process b1 set alias 'alias'");
        assertEquals(alias.getAliasedName(), new AliasedName("alias"));
    }
}
