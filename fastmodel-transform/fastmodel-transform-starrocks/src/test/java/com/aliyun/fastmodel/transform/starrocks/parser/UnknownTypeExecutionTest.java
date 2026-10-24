/*
 * Copyright 2026 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.starrocks.parser;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.starrocks.StarRocksTransformer;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnknownTypeExecutionTest {

    private final StarRocksLanguageParser parser = new StarRocksLanguageParser();
    private final StarRocksTransformer transformer = new StarRocksTransformer();

    @Test
    public void testUnknownAndNestedTypesRemainReadableButNotExecutable() {
        String[] types = {"UNKNOWN_TYPE", "ARRAY<UNKNOWN_TYPE>", "MAP<STRING, ARRAY<UNKNOWN_TYPE>>"};
        for (String type : types) {
            CreateTable table = (CreateTable)parser.parseNode("CREATE TABLE sample_types (payload " + type + ")");
            assertNotExecutable(table);
            ColumnDefinition column = table.getColumnDefines().get(0);
            assertNotExecutable(new AddCols(table.getQualifiedName(), ImmutableList.of(column)));
            assertNotExecutable(new ChangeCol(table.getQualifiedName(), column.getColName(), column));
            assertNotExecutable(new SetColumnOrder(table.getQualifiedName(), column.getColName(), column, null, true));
        }
    }

    @Test
    public void testStructFieldWithUnknownTypeIsNotExecutable() {
        RowDataType struct = new RowDataType(ImmutableList.of(new Field(new Identifier("value"),
            new StarRocksGenericDataType("UNKNOWN_TYPE"), null)));
        ColumnDefinition column = ColumnDefinition.builder().colName(new Identifier("payload")).dataType(struct).build();
        assertNotExecutable(CreateTable.builder().tableName(QualifiedName.of("sample_types"))
            .columns(ImmutableList.of(column)).build());
    }

    @Test
    public void testUnsupportedBatchRetainsEveryClause() {
        QualifiedName table = QualifiedName.of("sample_types");
        ColumnDefinition unknown = ColumnDefinition.builder().colName(new Identifier("payload"))
            .dataType(new StarRocksGenericDataType("UNKNOWN_TYPE")).build();
        ColumnDefinition known = ColumnDefinition.builder().colName(new Identifier("counter"))
            .dataType(new StarRocksGenericDataType("INT")).build();
        CompositeStatement changes = new CompositeStatement(ImmutableList.of(
            new ChangeCol(table, unknown.getColName(), unknown), new AddCols(table, ImmutableList.of(known))));
        DialectNode result = transformer.transform(changes);
        assertFalse(result.isExecutable());
        assertTrue(result.getNode().contains("MODIFY COLUMN payload UNKNOWN_TYPE"));
        assertTrue(result.getNode().contains("ADD COLUMN"));
        assertTrue(result.getNode().contains("counter INT"));
    }

    @Test
    public void testSupportedTypeRemainsExecutable() {
        CreateTable table = (CreateTable)parser.parseNode("CREATE TABLE sample_types (payload ARRAY<INT>)");
        assertTrue(transformer.transform(table).isExecutable());
    }

    private void assertNotExecutable(BaseStatement statement) {
        DialectNode result = transformer.transform(statement);
        assertFalse(result.getNode(), result.isExecutable());
        assertTrue(result.getNode(), result.getNode().contains("UNKNOWN_TYPE"));
    }
}
