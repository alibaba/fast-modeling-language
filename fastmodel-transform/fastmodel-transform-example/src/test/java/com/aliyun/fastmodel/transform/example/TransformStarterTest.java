/*
 * Copyright 2021-2023 Alibaba Group Holding Ltd.
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

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.dialect.MaxComputeVersion;
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
    public void testTransform() {
        String transform = example.transformMaxCompute(
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build());
        assertEquals("CREATE TABLE b;", transform);
    }

    @Test
    public void testTransformMysql() {
        String transform = example.transform(DialectMeta.DEFAULT_MYSQL,
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(),
            MysqlTransformContext.builder().build());
        assertEquals("CREATE TABLE b", transform);
    }

    @Test
    public void testCompare() {
        String before = " CREATE DIM TABLE IF NOT EXISTS example_project.example_table ALIAS '描述信息' (   demo ALIAS '描述信息' "
            + "STRING WITH ('uuid'='tb_c-00000000000000000000000000000001'),   demo2 ALIAS '测试demo' BIGINT WITH "
            + "('uuid'='tb_c-00000000000000000000000000000002'),   test2 ALIAS '字段名称' STRING CORRELATION NOT NULL "
            + "WITH ('uuid'='tb_c-00000000000000000000000000000003'))";
        String after
            = " CREATE DIM TABLE IF NOT EXISTS example_project.example_table ALIAS '描述信息' (   demo ALIAS '描述信息' STRING WITH "
            + "('uuid'='tb_c-00000000000000000000000000000001'),   demo2 ALIAS '测试demo' BIGINT WITH "
            + "('uuid'='tb_c-00000000000000000000000000000002'),   test2 ALIAS '字段名称' STRING CORRELATION NOT NULL "
            + "WITH ('uuid'='tb_c-00000000000000000000000000000003'))";
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        String s = example.compareAndTransform(fastModelParser.parseStatement(before),
            fastModelParser.parseStatement(after),
            DialectMeta.getMaxCompute(), MaxComputeContext.builder()
                .build());
        assertEquals("", s);
    }

    @Test
    public void testTransformWith1() {
        List<ColumnDefinition> column = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("10"))
            ).build(),
            ColumnDefinition.builder().colName(new Identifier("c2")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.DATE)
            ).build()
        );
        BaseStatement statement = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            column
        ).comment(new Comment("comment")).build();
        String s = example.transformMaxCompute(statement);
        assertEquals("CREATE TABLE b\n"
            + "(\n"
            + "   c1 VARCHAR(10),\n"
            + "   c2 DATE\n"
            + ")\n"
            + "COMMENT 'comment';", s);
    }

    @Test
    public void testDropTransform() {
        String s = example.compareAndTransform(null, new DropTable(QualifiedName.of("a.b")),
            DialectMeta.getMaxCompute(), MaxComputeContext.builder().appendSemicolon(true).build());
        assertEquals(s, "DROP TABLE b;");

    }

    @Test
    public void testDropTransformHive() {
        String s = example.compareAndTransform(null, new DropTable(QualifiedName.of("a.b")),
            DialectMeta.getHive(), HiveTransformContext.builder().build());
        assertEquals(s, "DROP TABLE b");

    }

    @Test
    public void testTransformWithColumn() {
        List<ColumnDefinition> columnDefines = ImmutableList.of(ColumnDefinition.builder().colName(
            new Identifier("c1")).dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name()))).comment(
            new Comment("c1")
        ).primary(true).notNull(true).comment(new Comment("c1")).build());
        ImmutableList<ColumnDefinition> of = ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("col1"))
            .dataType(
                new GenericDataType(new Identifier(DataTypeEnums.STRING
                    .name()))).comment(new Comment("comment")).primary(true).notNull(true).build());
        ImmutableList<Property> of1 = ImmutableList.of(new Property("key", "value"));
        Comment comment = new Comment("table_comment");
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
                QualifiedName.of("a.b")).detailType(TableDetailType.NORMAL_DIM).ifNotExist(true).columns(columnDefines)
            .partition(new PartitionedBy(of)).properties(of1).comment(comment).build();

        String s = example.transformMaxCompute(createDimTable);
        assertEquals(s, "CREATE TABLE IF NOT EXISTS b\n" +
            "(\n" +
            "   c1 BIGINT NOT NULL PRIMARY KEY COMMENT 'c1'\n" +
            ")\n" +
            "COMMENT 'table_comment'\n" +
            "PARTITIONED BY\n" +
            "(\n" +
            "   col1 STRING COMMENT 'comment'\n" +
            ")\n" +
            ";");
    }

    @Test
    public void testTransformChangeColumn() {
        String changeColumn = example.transformMaxCompute(new ChangeCol(
            QualifiedName.of("a.b"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c2"))
                .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name()))).comment(new Comment("abc"))
                .primary(true).build()
        ));
        assertEquals(changeColumn, "ALTER TABLE b CHANGE COLUMN c1 c2 BIGINT COMMENT 'abc';");
    }

    @Test
    public void testTransformHive() {
        TransformStarter example = new TransformStarter();
        String s = example.transformHive(CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build());
        assertEquals("CREATE TABLE b", s);
    }

    @Test
    public void testRename() {
        String transform = example.transformMaxCompute(new RenameTable(QualifiedName.of("b"), QualifiedName.of("e")));
        assertEquals("ALTER TABLE b RENAME TO e;", transform);
    }

    @Test
    public void testSetComment() {
        String result = example.transformMaxCompute(
            new SetTableComment(QualifiedName.of("a.b"), new Comment("newComment")));
        assertEquals("ALTER TABLE b SET COMMENT 'newComment';", result);
    }

    @Test
    public void testAddCols() {
        List<ColumnDefinition> columnList = ImmutableList.of(
            ColumnDefinition.builder().colName(
                    new Identifier("c")).
                dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name())))
                .comment(new Comment("Comment c")).build()
        );
        String result = example.transformMaxCompute(new AddCols(
            QualifiedName.of("a.b"),
            columnList
        ));
        assertEquals(result, "ALTER TABLE b ADD COLUMNS\n"
            + "(\n"
            + "   c BIGINT COMMENT 'Comment c'\n"
            + ");");
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
    public void testTransformWithDialectMeta() {
        String transform = example.transform(DialectMeta.getByNameAndVersion(DialectName.MAXCOMPUTE.name(), MaxComputeVersion.V2),
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(),
            MaxComputeContext.builder().appendSemicolon(true)
                .build()
        );
        assertEquals(transform, "CREATE TABLE b;");

        transform = example.transform(DialectMeta.getByNameAndVersion(DialectName.MAXCOMPUTE.name(), MaxComputeVersion.V1),
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build(),
            MaxComputeContext.builder().appendSemicolon(true).build()
        );
        assertEquals(transform, "CREATE TABLE b;");

        transform = example.transform(DialectMeta.getByNameAndVersion(DialectName.MAXCOMPUTE.name(), MaxComputeVersion.V1),
            new SetTableComment(QualifiedName.of("a.b"), new Comment("bcd")),
            MaxComputeContext.builder().appendSemicolon(true).build()
        );
        assertEquals(transform, "ALTER TABLE b SET COMMENT 'bcd';");

    }

    @Test
    public void testCompareIssue() {
        String before = "CREATE FACT TABLE IF NOT EXISTS example_project.example_events ALIAS '测试' \n"
            + "(\n"
            + "   field1 ALIAS '测试' STRING NOT NULL WITH ('uuid'='tb_c-00000000000000000000000000000004'),\n"
            + "   field2 ALIAS '测试2' STRING NOT NULL WITH ('uuid'='tb_c-00000000000000000000000000000005'),\n"
            + "   CONSTRAINT pk PRIMARY KEY(field1)\n"
            + ")\n";
        String after = "CREATE FACT TABLE IF NOT EXISTS example_project.example_events ALIAS '测试' \n"
            + "(\n"
            + "   field2 ALIAS '测试' STRING NOT NULL WITH ('uuid'='tb_c-00000000000000000000000000000004'),\n"
            + "   CONSTRAINT PK PRIMARY KEY(field2)\n"
            + ")";
        List<BaseStatement> list = example.compare(before, after);
        String s = example.transformMaxCompute(list, "\n");
        assertEquals(s, "ALTER TABLE example_events CHANGE COLUMN field1 field2 STRING;\n"
            + "ALTER TABLE example_events CHANGE COLUMN field2 field2 STRING;\n"
            + "ALTER TABLE example_events CHANGE COLUMN field2 field2 STRING FIRST;\n"
            + "ALTER TABLE example_events DROP CONSTRAINT pk;\n"
            + "ALTER TABLE example_events ADD CONSTRAINT pk PRIMARY KEY(field2);");
    }

    @Test
    public void testCompareCodeTable() {
        String before = "CREATE CODE TABLE IF NOT EXISTS example_codes ALIAS '示例代码' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING ATTRIBUTE NOT NULL COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING ATTRIBUTE COMMENT 'name',\n"
            + "   extendname ALIAS 'extendName' STRING ATTRIBUTE COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING ATTRIBUTE COMMENT 'description'\n"
            + ")\n"
            + " COMMENT '合成代码表示例'";
        String after = "CREATE CODE TABLE IF NOT EXISTS example_codes ALIAS '示例代码' \n"
            + "(\n"
            + "   code ALIAS 'code' STRING COMMENT 'code',\n"
            + "   name ALIAS 'name' STRING COMMENT 'name',\n"
            + "   extendName ALIAS 'extendName' STRING COMMENT 'extendName',\n"
            + "   description ALIAS 'description' STRING COMMENT 'description',\n"
            + "   CONSTRAINT pk_code PRIMARY KEY(code)\n"
            + ")\n"
            + " COMMENT '合成代码表示例'";
        List<BaseStatement> list = example.compare(before, after);
        assertEquals(list.size(), 1);
        BaseStatement baseStatement = list.get(0);
        assertEquals(baseStatement.toString(),
            "ALTER TABLE example_codes ADD CONSTRAINT pk_code PRIMARY KEY(code)");
    }
}