/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author panguanjing
 * @date 2020/11/26
 */
public class McTransformFromTest {

    MaxComputeTransformer mcTransformer = new MaxComputeTransformer();

    @Test
    public void testFrom() {
        ReverseContext context = ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build();
        CreateDimTable createTable =
            (CreateDimTable)mcTransformer.reverse(
                new DialectNode(
                    "CREATE TABLE if not exists ab.test  (a string not null default 'abc', b string primary key) "
                        + "comment 'comment';"),
                context);
        assertNotNull(createTable);
        assertEquals(createTable.getQualifiedName(), QualifiedName.of("ab.test"));
        assertEquals(createTable.getComment(), new Comment("comment"));
        assertEquals(createTable.getColumnDefines().size(), 2);
        assertEquals(createTable.getColumnDefines().get(0).getDefaultValue(), new StringLiteral("abc"));
        assertEquals(createTable.getConstraintStatements().size(), 1);
    }

    @Test
    public void testWithProperty() {
        ReverseContext context = ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build();
        CreateTable createTable = (CreateTable)mcTransformer.reverse(
            new DialectNode(
                "CREATE TABlE ab.test (a string primary key) comment 'comment' TBLPROPERTIES('key' = 'value');")
            , context
        );
        assertEquals(createTable.getQualifiedName(), QualifiedName.of("ab.test"));
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        assertEquals(1, constraintStatements.size());
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)constraintStatements.get(0);
        assertEquals(primaryConstraint.getColNames().get(0), new Identifier("a"));
        assertEquals(createTable.getProperties().size(), 1);
    }

}
