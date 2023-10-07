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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.dict.CreateDict;
import com.aliyun.fastmodel.core.tree.statement.dict.DropDict;
import com.aliyun.fastmodel.core.tree.statement.dict.RenameDict;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictAlias;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictComment;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author panguanjing
 * @date 2020/11/13
 */
public class DictTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testCreate() {
        String sql
            = "create dict taobao.name bigint not null default 'abc' comment 'comment' WITH('memo' = 'beizhu') ";
        CreateDict createAdjunct = (CreateDict)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(createAdjunct.getBusinessUnit(), "taobao");
        assertEquals(createAdjunct.getComment(), new Comment("comment"));
        assertEquals(createAdjunct.getBaseDataType().getTypeName(), DataTypeEnums.BIGINT);
        assertTrue(createAdjunct.notNull());
        assertEquals(createAdjunct.getDefaultValue(), new StringLiteral("abc"));
    }

    @Test
    public void testSetComment() {
        String sql = "alter dict taobao.name set comment 'comment'";
        SetDictComment setAdjunctComment = (SetDictComment)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(setAdjunctComment.getComment(), new Comment("comment"));
    }

    @Test
    public void testRenameAdjunct() {
        String sql = "alter dict taobao.name rename to taobao.name2";
        RenameDict renameAdjunct = (RenameDict)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(renameAdjunct.getNewIdentifier(), "name2");
    }

    @Test
    public void testSetProperties() {
        SetDictProperties setAdjunctProperties = (SetDictProperties)nodeParser.parse(new DomainLanguage(
            "alter dict taobao.name int null default null set ('type'='type1')"
        ));
        assertEquals(setAdjunctProperties.getProperties().size(), 1);
        BaseDataType baseDataType = setAdjunctProperties.getBaseDataType();
        assertEquals(baseDataType.getTypeName(), DataTypeEnums.INT);
        BaseConstraint baseConstraint = setAdjunctProperties.getBaseConstraint();
        assertNotNull(baseConstraint);
        NotNullConstraint notNullConstraint = (NotNullConstraint)baseConstraint;
        assertFalse(notNullConstraint.getEnable());
    }

    @Test
    public void testDropDict() {
        DropDict dropDict = (DropDict)nodeParser.parse(new DomainLanguage("drop dict a.b"));
        assertEquals(dropDict.getQualifiedName(), QualifiedName.of("a.b"));
    }

    @Test
    public void testSetDictAlias() {
        SetDictAlias setDictAlias = nodeParser.parseStatement("ALTER DICT dc SET ALIAS 'alias'");
        assertEquals(setDictAlias.getAliasedName(), new AliasedName("alias"));
    }
}
