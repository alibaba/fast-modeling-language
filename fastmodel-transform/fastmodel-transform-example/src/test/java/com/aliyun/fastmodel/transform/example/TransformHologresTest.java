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

package com.aliyun.fastmodel.transform.example;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/17
 */
public class TransformHologresTest {
    TransformStarter starter = new TransformStarter();

    @Test
    public void testCreateTable() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder().colName(new Identifier("c1"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).comment(new Comment("comment")).build()
            )).comment(new Comment("table comment")).build();
        String result = starter.transformHologres(createDimTable);
        assertEquals(result, "BEGIN;\n"
            + "CREATE TABLE b (\n"
            + "   c1 BIGINT\n"
            + ");\n"
            + "COMMENT ON TABLE b IS 'table comment';\n"
            + "COMMENT ON COLUMN b.c1 IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testAddCol() {
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                ).build(), ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.DATETIME)).primary(true).comment(new Comment("table comment"))
                    .build()
            ));
        String s = starter.transformHologres(addCols);
        assertEquals("BEGIN;\n"
            + "ALTER TABLE IF EXISTS b ADD COLUMN col1 BIGINT, ADD COLUMN col2 TIMESTAMP;\n"
            + "COMMENT ON COLUMN b.col2 IS 'table comment';\n"
            + "COMMIT;", s);
    }

    @Test
    public void testDropTable() {
        DropTable dropTable = new DropTable(
            QualifiedName.of("a.b")
        );
        String s = starter.transformHologres(dropTable);
        assertEquals("DROP TABLE IF EXISTS b;", s);
    }

    @Test
    public void testDropCol() {
        DropCol dropCol = new DropCol(
            QualifiedName.of("a.b"),
            new Identifier("c1")
        );
        String s = starter.transformHologres(dropCol);
        assertEquals(s, "ALTER TABLE b DROP COLUMN c1;");
    }
   
    @Test
    public void testSetProperties() {
        SetTableProperties setTableProperties = new SetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of(new Property("key", "value"))
        );
        String set = starter.transformHologres(setTableProperties);
        assertEquals("", set);
    }
}
