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
import com.aliyun.fastmodel.core.tree.statement.measure.unit.CreateMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.DropMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.RenameMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitAlias;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitComment;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author panguanjing
 * @date 2020/11/13
 */
public class MeasureUnitTest {

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testCreate() {
        String sql
            = "create measure_unit if not exists taobao.name comment 'comment' with properties('type'='abc')";
        CreateMeasureUnit parse = (CreateMeasureUnit)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(parse.getBusinessUnit(), "taobao");
        assertEquals(parse.getComment(), new Comment("comment"));
    }

    @Test
    public void testCreateWithNoPropertyKey() {
        String sql = " create measure_unit t.n with ('type'='abc')";
        CreateMeasureUnit p = (CreateMeasureUnit)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(p.getProperties().size(), 1);
    }

    @Test
    public void testSetComment() {
        String sql = "alter measure_unit taobao.name set comment 'comment'";
        SetMeasureUnitComment setAdjunctComment = (SetMeasureUnitComment)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(setAdjunctComment.getComment(), new Comment("comment"));
    }

    @Test
    public void testRename() {
        String sql = "alter measure_unit taobao.name rename to taobao.name2";
        RenameMeasureUnit renameAdjunct = (RenameMeasureUnit)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(renameAdjunct.getNewIdentifier(), "name2");
    }

    @Test
    public void testSetProperties() {
        SetMeasureUnitProperties setTimePeriodProperties = (SetMeasureUnitProperties)nodeParser.parse(
            new DomainLanguage(
                "alter measure_unit taobao.name set ('type'='type1')"
            ));
        assertEquals(setTimePeriodProperties.getProperties().size(), 1);
    }

    @Test
    public void testDrop() {
        DropMeasureUnit timePeriod = (DropMeasureUnit)nodeParser.parse(
            new DomainLanguage("drop measure_unit taobao.name"));
        assertNotNull(timePeriod.getQualifiedName());
    }

    @Test
    public void testSetAlias() {
        SetMeasureUnitAlias setMeasureUnitAlias = nodeParser.parseStatement("alter measure_unit na set alias 'alias'");
        assertEquals(setMeasureUnitAlias.getAliasedName(), new AliasedName("alias"));
    }
}
