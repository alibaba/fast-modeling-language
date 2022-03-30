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
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.layer.AddChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.Checker;
import com.aliyun.fastmodel.core.tree.statement.layer.CreateLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.DropChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.DropLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.RenameLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerAlias;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerComment;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author panguanjing
 * @date 2020/12/1
 */
public class LayerTest {
    private final NodeParser nodeParser = new NodeParser();

    @Test
    public void testCreate() {
        String s = "create layer if not exists ut.layerName (checker regex r1 '+d' comment 'abc')";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(s));
        CreateLayer createLayer = (CreateLayer)parse;
        assertEquals(createLayer.getQualifiedName(), QualifiedName.of("ut.layername"));
        assertEquals(createLayer.getCheckers().size(), 1);
        Checker checker = createLayer.getCheckers().get(0);
        assertEquals(checker.getCheckerName(), new Identifier("r1"));
        assertEquals(checker.getComment(), new Comment("abc"));
    }

    @Test
    public void testCreateWithoutProperties() {
        String sql = "create layer ut.layerName with ('a'='b')";
        CreateLayer createLayer = (CreateLayer)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(createLayer.getProperties().size(), 1);
        assertEquals(createLayer.getStatementType(), StatementType.LAYER);
    }

    @Test
    public void testSetLayerComment() {
        String s = "alter layer ut.layerName set comment 'abc'";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(s));
        SetLayerComment setLayerComment = (SetLayerComment)parse;
        assertEquals(setLayerComment.getComment(), new Comment("abc"));
        assertEquals(setLayerComment.getStatementType(), StatementType.LAYER);
    }

    @Test
    public void testRename() {
        String s = "alter layer ut.l1 rename to ut.l2";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(s));
        RenameLayer renameLayer = (RenameLayer)parse;
        assertEquals(renameLayer.getTarget(), QualifiedName.of("ut.l2"));
        assertEquals(renameLayer.getStatementType(), StatementType.LAYER);
    }

    @Test
    public void testDrop() {
        String s = "drop layer ut.l1";
        DropLayer layer = (DropLayer)nodeParser.parse(new DomainLanguage(s));
        assertEquals(layer.getQualifiedName(), QualifiedName.of("ut.l1"));
        assertEquals(layer.getStatementType(), StatementType.LAYER);
    }

    @Test
    public void testAddChecker() {
        String s
            = " alter layer ut.l1 add checker regex r1 'd+' comment 'regex'";
        AddChecker addChecker = (AddChecker)nodeParser.parse(new DomainLanguage(s));
        assertNotNull(addChecker);
        assertEquals(addChecker.getStatementType(), StatementType.LAYER);
        assertEquals(addChecker.getChecker().getExpression().getValue(), "d+");
    }

    @Test
    public void testDropChecker() {
        String s = "alter layer ut.l1 drop checker r1";
        DropChecker dropChecker = (DropChecker)nodeParser.parse(new DomainLanguage(s));
        assertEquals(dropChecker.getCheckerName(), new Identifier("r1"));
    }

    @Test
    public void testSetLayerProperties() {
        String fml = "alter layer ut.l1 set properties('k1'='v1')";
        SetLayerProperties setLayerProperties = (SetLayerProperties)nodeParser.parse(new DomainLanguage(fml));
        assertEquals(setLayerProperties.getProperties(), ImmutableList.of(new Property("k1", "v1")));
    }

    @Test
    public void testSetAliasLayer() {
        String fml = "alter layer t1 set alias 'alias'";
        SetLayerAlias setLayerAlias = (SetLayerAlias)nodeParser.parse(new DomainLanguage(fml));
        assertEquals(setLayerAlias.getAliasedName(), new AliasedName("alias"));
    }
}
