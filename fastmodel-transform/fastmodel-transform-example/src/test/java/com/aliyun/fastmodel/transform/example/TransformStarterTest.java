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

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/7
 */
public class TransformStarterTest {

    TransformStarter example = new TransformStarter();

    @Test
    public void testTransformMysql() {
        String transform = example.transform(DialectMeta.DEFAULT_MYSQL,
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(),
            MysqlTransformContext.builder().build());
        assertEquals("CREATE TABLE b", transform);
    }

    @Test
    public void testDropTransformHive() {
        String s = example.compareAndTransform(null, new DropTable(QualifiedName.of("a.b")),
            DialectMeta.getHive(), HiveTransformContext.builder().build());
        assertEquals(s, "DROP TABLE b");

    }

    @Test
    public void testTransformHive() {
        TransformStarter example = new TransformStarter();
        String s = example.transformHive(CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build());
        assertEquals("CREATE TABLE b", s);
    }

    @Test
    public void testHiveAddCols() {
        String c1 = example.transformHive(new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList
                .of(ColumnDefinition.builder().colName(new Identifier("c1"))
                    .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT
                        .name()))).build())
        ));
        assertEquals(c1, "ALTER TABLE b ADD COLUMNS\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")");
    }

    @Test
    public void testCompareCodeTable() {
        String before = "CREATE CODE TABLE IF NOT EXISTS administrative_area ALIAS '行政区划代码' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING ATTRIBUTE NOT NULL COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING ATTRIBUTE COMMENT 'name',\n"
            + "   extendname ALIAS 'extendName' STRING ATTRIBUTE COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING ATTRIBUTE COMMENT 'description'\n"
            + ")\n"
            + " COMMENT '2020年11月中华人民共和国县以上行政区划代码'";
        String after = "CREATE CODE TABLE IF NOT EXISTS administrative_area ALIAS '行政区划代码' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING COMMENT 'name',\n"
            + "   extendName ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + " COMMENT '2020年11月中华人民共和国县以上行政区划代码'";
        List<BaseStatement> list = example.compare(before, after);
        assertEquals(list.size(), 1);
        BaseStatement baseStatement = list.get(0);
        assertEquals(baseStatement.toString(),
            "ALTER TABLE administrative_area ADD CONSTRAINT pk_code PRIMARY KEY(code)");
    }
}