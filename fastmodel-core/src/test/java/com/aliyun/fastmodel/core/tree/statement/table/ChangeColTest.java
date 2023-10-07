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

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author panguanjing
 * @date 2020/11/17
 */
public class ChangeColTest {

    ChangeCol renameCol = new ChangeCol(
        QualifiedName.of("a"),
        new Identifier("old"),
        ColumnDefinition.builder().colName(new Identifier("new")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("1")
            )).comment(new Comment("comment")).primary(false).notNull(true).build());

    @Test
    public void testConstraint() {
        assertFalse(renameCol.getPrimary());
        assertTrue(renameCol.getNotNull());
        assertTrue(renameCol.toString().contains("new"));
    }

    @Test
    public void testEquals() {
        ChangeCol renameCol2 = new ChangeCol(
            QualifiedName.of("a"),
            new Identifier("old"),
            ColumnDefinition.builder().colName(new Identifier("new"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("1"))).comment(
                new Comment("comment")).primary(false).notNull(true).build());
        assertEquals(renameCol, renameCol2);
    }
}