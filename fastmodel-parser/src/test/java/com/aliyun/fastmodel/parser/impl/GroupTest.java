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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.group.CreateGroup;
import com.aliyun.fastmodel.core.tree.statement.group.DropGroup;
import com.aliyun.fastmodel.core.tree.statement.group.GroupType;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupAlias;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupComment;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupProperties;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/8
 */
public class GroupTest extends BaseTest {

    @Test
    public void testCreateGroup() {
        String sql = "create group dict a.b alias 'alias'";
        CreateGroup parse = parse(sql, CreateGroup.class);
        assertEquals(parse.getQualifiedName(), QualifiedName.of("a.b"));
        assertEquals(parse.getGroupType(), GroupType.DICT);
        assertEquals(parse.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testCreateGroup2() {
        String fml = "create group dict a.b.c alias 'alias'";
        CreateGroup parse = parse(fml, CreateGroup.class);
        assertEquals(parse.getIdentifier(), "b.c");
    }

    @Test
    public void testSetCommentGroup() {
        String sql = "alter group dict a.b set comment 'abc'";
        SetGroupComment setGroupComment = parse(sql, SetGroupComment.class);
        assertEquals(setGroupComment.getComment(), new Comment("abc"));
        assertEquals(setGroupComment.getGroupType(), GroupType.DICT);
    }

    @Test
    public void testSetProperties() {
        String sql = "alter group dict a.b set ('k1'='v1')";
        SetGroupProperties setGroupProperties = parse(sql, SetGroupProperties.class);
        assertEquals(setGroupProperties.getGroupType(), GroupType.DICT);
    }

    @Test
    public void testDrop() {
        String sql = "drop group dict a.b";
        DropGroup dropGroup = parse(sql, DropGroup.class);
        assertEquals(dropGroup.getQualifiedName(), QualifiedName.of("a.b"));
    }

    @Test
    public void testSetAlias() {
        String sql = "alter group dict group_name set alias 'alias'";
        SetGroupAlias setGroupAlias = parse(sql, SetGroupAlias.class);
        assertEquals(setGroupAlias.getAliasedName(), new AliasedName("alias"));
    }

    @Test
    public void testGroupCode() {
        String fml = "create group code code_name alias 'alias'";
        CreateGroup createGroup = parse(fml, CreateGroup.class);
        GroupType groupType = createGroup.getGroupType();
        assertEquals(groupType, GroupType.CODE);
    }
}
