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

package com.aliyun.fastmodel.transform.hologres;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/7
 */
public class HologresTransformerTest {

    HologresTransformer hologresTransformer = new HologresTransformer();

    @Test
    public void testTransform() {
        CreateDimTable dimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        DialectNode transform = hologresTransformer.transform(dimTable, HologresTransformContext.builder().build());
        assertNotNull(transform.getNode());
    }

    @Test
    public void testChangeColumn() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                            .colName(new Identifier("bcd"))
                            .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name())))
                            .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE abc CHANGE COLUMN bcd bcd BIGINT");
        assertFalse(transform.isExecutable());
    }

    @Test
    public void testChangeColumn_rename() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                            .colName(new Identifier("bde"))
                            .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name())))
                            .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE abc RENAME COLUMN bcd TO bde");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testChangeColumn_setdefault() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                            .colName(new Identifier("bcd"))
                            .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name())))
                            .defaultValue(new StringLiteral("bcd"))
                            .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE abc ALTER COLUMN bcd SET DEFAULT 'bcd'");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testTransform_map() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        CreateDimTable dimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).columns(
            ImmutableList.of(
                ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(DataTypeEnums.MAP))
                                .colName(new Identifier("col1")).build()
            )

        ).build();
        DialectNode transform = hologresTransformer.transform(dimTable, HologresTransformContext.builder().build());
        assertEquals("BEGIN;\nCREATE TABLE b (\n"
            + "   col1 JSON\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('b', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('b', 'time_to_live_in_seconds', '3153600000');\n\nCOMMIT;", transform.getNode());
    }

    @Test
    public void testTransform_addColumn() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        AddCols addCols = new AddCols(
            QualifiedName.of("abc"),
            Lists.newArrayList(
                ColumnDefinition.builder()
                                .colName(new Identifier("abc"))
                                .dataType(new GenericDataType(new Identifier("BIGINT")))
                                .build()
            )
        );
        DialectNode transform = hologresTransformer.transform(addCols);
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "ALTER TABLE IF EXISTS abc ADD COLUMN abc BIGINT;\n"
            + "\n"
            + "COMMIT;");
        assertTrue(transform.isExecutable());
    }

}