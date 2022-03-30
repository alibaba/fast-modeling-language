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

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.script.ImportObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * script test
 *
 * @author panguanjing
 * @date 2021/9/14
 */
public class ScriptTest extends BaseTest {

    @Test
    public void testParser() {
        String fml = "import dim_shop as d";
        ImportObject parse = parse(fml, ImportObject.class);
        Identifier alias = parse.getAlias();
        assertEquals(new Identifier("d"), alias);
        assertEquals(parse.toString(), "IMPORT dim_shop AS d");
    }

    @Test
    public void testRef() {
        String fml = "REF t1 -> t2 : 关系";
        RefRelation refEntityStatement = parse(fml, RefRelation.class);
        RefObject leftTable = refEntityStatement.getLeft();
        QualifiedName tableName = leftTable.getMainName();
        assertEquals(tableName, QualifiedName.of("t1"));
        assertEquals(refEntityStatement.toString(), "REF t1 -> t2 : 关系");
    }

    @Test
    public void testRefVertical() {
        String fml = "REF t1 >-> t2 : 关系";
        RefRelation refEntityStatement = parse(fml, RefRelation.class);
        RefObject leftTable = refEntityStatement.getLeft();
        QualifiedName tableName = leftTable.getMainName();
        assertEquals(tableName, QualifiedName.of("t1"));
        assertEquals(refEntityStatement.toString(), "REF t1 >-> t2 : 关系");
    }

    @Test
    public void testRefVerticalRightToLeft() {
        String fml = "REF t1 <-< t2 : 关系";
        RefRelation refEntityStatement = parse(fml, RefRelation.class);
        RefObject leftTable = refEntityStatement.getLeft();
        QualifiedName tableName = leftTable.getMainName();
        assertEquals(tableName, QualifiedName.of("t1"));
        assertEquals(refEntityStatement.toString(), "REF t1 <-< t2 : 关系");
    }

    @Test
    public void testComment() {
        String fml = "REF t1.a, t1.b -> t2.a, t2.b : 关系";
        RefRelation refRelation = parse(fml, RefRelation.class);
        RefDirection refDirection = refRelation.getRefDirection();
        assertEquals(refDirection.getCode(), RefDirection.LEFT_DIRECTION_RIGHT.getCode());
        RefObject left = refRelation.getLeft();
        QualifiedName tableName = left.getMainName();
        assertEquals(tableName.toString(), "t1");
        List<Identifier> columnList = left.getAttrNameList();
        assertEquals(columnList.get(0), new Identifier("a"));

        RefObject right = refRelation.getRight();
        QualifiedName tableName1 = right.getMainName();
        assertEquals(tableName1.toString(), "t2");
    }

    @Test
    public void testComment2() {
        String fml = "REF t1.a, t1.b COMMENT 'a' ->  t2.a, t2.b COMMENT 'b': 关系";
        RefRelation refRelation = parse(fml, RefRelation.class);
        RefDirection refDirection = refRelation.getRefDirection();
        assertEquals(refDirection.getCode(), RefDirection.LEFT_DIRECTION_RIGHT.getCode());
        RefObject left = refRelation.getLeft();
        QualifiedName tableName = left.getMainName();
        assertEquals(tableName.toString(), "t1");
        List<Identifier> columnList = left.getAttrNameList();
        assertEquals(columnList.get(0), new Identifier("a"));

        RefObject right = refRelation.getRight();
        QualifiedName tableName1 = right.getMainName();
        assertEquals(tableName1.toString(), "t2");

        Comment comment = left.getComment();
        assertEquals(comment.getComment(), "a");
    }
}
