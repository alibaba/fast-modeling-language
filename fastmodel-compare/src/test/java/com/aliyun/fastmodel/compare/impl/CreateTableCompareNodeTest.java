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

package com.aliyun.fastmodel.compare.impl;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/2
 */
public class CreateTableCompareNodeTest {
    CreateTableCompareNode createTableCompareNode = new CreateTableCompareNode();

    @Test
    public void testCompare() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        CreateDimTable createDimTable1 = CreateDimTable.builder().tableName(
            QualifiedName.of("a.c")
        ).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b RENAME TO a.c");
    }

    private String getExpected(BaseStatement statement) {
        return FastModelFormatter.formatNode(statement);
    }

    @Test
    public void testBeforeIsNull() {
        CreateDimTable after = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(null, after, CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(statement, after);
        assertEquals(getExpected(statement), "CREATE DIM TABLE a.b");
    }

    @Test
    public void testAfterIsNull() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, null,
            CompareStrategy.INCREMENTAL);
        assertEquals(statements.size(), 1);
        BaseStatement statement = statements.get(0);
        assertEquals(statement.getClass(), DropTable.class);
    }

    @Test
    public void testCompareProperties() {
        List<Property> beforeProperties = ImmutableList.of(new Property("a", "a1"), new Property("b", "b1"));
        List<Property> afterProperties = ImmutableList.of(new Property("b", "b1"), new Property("c", "c1"));

        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).properties(
            beforeProperties
        ).build();
        CreateDimTable afterDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).properties(
            afterProperties).build();
        List<BaseStatement> statements =
            createTableCompareNode.compareResult(createDimTable, afterDimTable, CompareStrategy.INCREMENTAL);
        assertEquals(2, statements.size());

        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b UNSET PROPERTIES('a')");
        statement = statements.get(1);
        assertEquals(getExpected(statement), "ALTER TABLE a.b SET PROPERTIES('c'='c1')");
    }

    @Test
    public void testCompareColumn() {
        List<ColumnDefinition> list = new ArrayList<>();
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        ColumnDefinition col1 = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            dataType).build();
        list.add(col1);
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).detailType(
            TableDetailType.NORMAL_DIM).columns(list).comment(new Comment("comment")).build();
        List<ColumnDefinition> list2 = new ArrayList<>();
        BaseDataType dataType1 = DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("1"));
        list2.add(ColumnDefinition.builder().colName(new Identifier("col2"))
            .dataType(dataType1).build());

        CreateDimTable createDimTable1 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).detailType(
            TableDetailType.NORMAL_DIM).columns(list2).comment(new Comment("comment")).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        assertEquals(2, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b DROP COLUMN col1");
        statement = statements.get(1);
        assertEquals(getExpected(statement), "ALTER TABLE a.b ADD COLUMNS\n"
            + "(\n"
            + "   col2 VARCHAR(1)\n"
            + ")");
    }

    @Test
    public void testCompareChangeColumn() {
        List<ColumnDefinition> list = new ArrayList<>();
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        list.add(ColumnDefinition.builder().colName(new Identifier("col1"))
            .dataType(dataType).build());

        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).columns(list).comment(new Comment("a")).build();

        List<ColumnDefinition> list2 = new ArrayList<>();
        BaseDataType dataType1 = DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("1"));
        list2.add(ColumnDefinition.builder().colName(new Identifier("col1"))
            .dataType(dataType1).build());

        CreateDimTable createDimTable1 = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).columns(list2).comment(new Comment("a")).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);
        BaseStatement st = statements.get(0);
        assertEquals(getExpected(st), "ALTER TABLE a.b CHANGE COLUMN col1 col1 VARCHAR(1)");
    }

    @Test
    public void testCompareComment() {
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).detailType(
            TableDetailType.NORMAL_DIM).comment(new Comment("a")).build();
        CreateDimTable after = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).detailType(TableDetailType.NORMAL_DIM).comment(new Comment("b")).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        BaseStatement st = statements.get(0);
        assertEquals(getExpected(st), "ALTER TABLE a.b SET COMMENT 'b'");
    }

    @Test
    public void testComparePartition() {
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        PartitionedBy p1 = new PartitionedBy(ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                    dataType)
                .build()));
        PartitionedBy p2 = new PartitionedBy(ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2"))
                .dataType(dataType).build()));
        CreateDimTable createDimTable
            = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).comment(
            new Comment("comment")
        ).partition(p1).build();

        CreateDimTable createDimTable1
            = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).comment(
            new Comment("comment")
        ).partition(p2).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable1,
            CompareStrategy.INCREMENTAL);

        BaseStatement statement = statements.get(0);
        assertEquals(getExpected(statement), "ALTER TABLE a.b DROP PARTITION COLUMN col1");
        statement = statements.get(1);
        assertEquals(getExpected(statement), "ALTER TABLE a.b ADD PARTITION COLUMN col2 BIGINT");
    }

    @Test
    public void testCompareConstraint() {
        List<BaseConstraint> constraints =
            ImmutableList.of(
                new PrimaryConstraint(new Identifier("abc"),
                    ImmutableList.of(new Identifier("col1"), new Identifier("col2")))
            );
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        List<ColumnDefinition> list = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                dataType).build(),
            ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                dataType).build()
        );
        CreateDimTable createDimTable
            = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(list).constraints(constraints)
            .build();

        List<BaseConstraint> constraints2 = ImmutableList.of();
        CreateDimTable createDimTable2
            = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(list).constraints(constraints2)
            .build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable2,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        assertEquals(getExpected(statements.get(0)), "ALTER TABLE a.b DROP CONSTRAINT abc");
    }

    @Test
    public void testCompareConstraintWithAdd() {
        List<BaseConstraint> constraints = ImmutableList.of();
        CreateDimTable createDimTable
            = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).constraints(constraints).build();
        List<BaseConstraint> constraints2 = ImmutableList.of(
            new DimConstraint(new Identifier("c1"), QualifiedName.of("a.b"))
        );
        CreateDimTable createDimTable2
            = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).constraints(constraints2).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, createDimTable2,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        assertEquals(getExpected(statements.get(0)), "ALTER TABLE a.b ADD CONSTRAINT c1 REFERENCES a.b");
    }

    @Test
    public void testCompareWithUuid() {
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        List<ColumnDefinition> leftList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                    dataType)
                .properties(ImmutableList.of(new Property("uuid", "uuid1"))).build()
        );

        List<ColumnDefinition> rightList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                dataType).properties(
                ImmutableList.of(new Property("uuid", "uuid1"))
            ).build()
        );
        CreateDimTable beforeDim =
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(leftList).comment(new Comment("a"))
                .build();
        CreateDimTable afterDim = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(rightList)
            .build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(beforeDim, afterDim,
            CompareStrategy.INCREMENTAL
        );
        assertEquals(2, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(statement.toString(), "ALTER TABLE a.b CHANGE COLUMN col1 col2 BIGINT");
        statement = statements.get(1);
        assertEquals(statement.toString(), "ALTER TABLE a.b SET COMMENT ''");
    }

    @Test
    public void testCompareWithUuidSetComment() {
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        List<ColumnDefinition> leftList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                dataType).comment(
                new Comment("abc")).properties(
                ImmutableList.of(new Property("uuid", "uuid1"))
            ).build()
        );

        List<ColumnDefinition> rightList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                dataType
            ).comment(new Comment("bcd")).properties(ImmutableList.of(new Property("uuid", "uuid1"))
            ).build()
        );
        CreateDimTable beforeDim =
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(leftList).comment(new Comment("a"))
                .build();
        CreateDimTable afterDim = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(rightList)
            .comment(
                new Comment("a")
            ).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(beforeDim, afterDim,
            CompareStrategy.INCREMENTAL
        );
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(statement.toString(), "ALTER TABLE a.b CHANGE COLUMN col2 col2 BIGINT COMMENT 'bcd'");
    }

    @Test
    public void testCompareWithUuidWithChange() {
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        List<ColumnDefinition> leftList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                    dataType).comment(new Comment("abc"))
                .properties(ImmutableList.of(new Property("uuid", "uuid1"))).build()
        );

        List<ColumnDefinition> rightList = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
                dataType
            ).comment(
                new Comment("bcd")
            ).properties(
                ImmutableList.of(new Property("uuid", "uuid1"))
            ).build()
        );
        CreateDimTable beforeDim =
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(leftList).comment(new Comment("a"))
                .build();
        CreateDimTable afterDim = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            rightList
        ).comment(
            new Comment("a")
        ).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(beforeDim, afterDim,
            CompareStrategy.INCREMENTAL
        );
        assertEquals(1, statements.size());
        BaseStatement statement = statements.get(0);
        assertEquals(statement.toString(), "ALTER TABLE a.b CHANGE COLUMN col1 col2 BIGINT COMMENT 'bcd'");
    }

    @Test
    public void addPartitionBy() {
        CreateDimTable beforeDim = CreateDimTable.builder().tableName(
            QualifiedName.of("a")).comment(new Comment("comment")).build();

        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.BIGINT);
        ImmutableList<ColumnDefinition> p1 = ImmutableList
            .of(ColumnDefinition.builder().colName(new Identifier("p1"))
                .dataType(dataType).build());
        CreateDimTable afterDim = CreateDimTable.builder().tableName(
            QualifiedName.of("a")).comment(new Comment("comment")).partition(new PartitionedBy(p1)).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(beforeDim, afterDim,
            CompareStrategy.INCREMENTAL);
        assertEquals(statements.size(), 1);
        assertEquals(statements.get(0).toString(), "ALTER TABLE a ADD PARTITION COLUMN p1 BIGINT");
    }

    @Test
    public void testPropertiesWithNullAndEmpty() {
        QualifiedName tableName = QualifiedName.of("dim_shop");
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col1")).notNull(true)
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).properties(ImmutableList.of(
                    new Property(
                        ColumnPropertyDefaultKey.uuid.name(), "uuid"))).build()
        );
        CreateDimTable before = CreateDimTable.builder().tableName(tableName).columns(columns).build();
        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col2")).notNull(false)
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).properties(
                    ImmutableList.of(new Property(ColumnPropertyDefaultKey.uuid.name(), "uuid"))
                ).build()
        );
        CreateDimTable after = CreateDimTable.builder().tableName(tableName).columns(columns2).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(statements.get(0).toString(), "ALTER TABLE dim_shop CHANGE COLUMN col1 col2 BIGINT NULL");
    }

    @Test
    public void testColumnPrimary() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).primary(false).build()
        );
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();

        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).primary(null).build()
        );
        CreateDimTable after = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns2
        ).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(0, statements.size());
    }

    @Test
    public void testColumnNotNull() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(false).build()
        );
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();

        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(null).build()
        );
        CreateDimTable after = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns2
        ).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(0, statements.size());
    }

    @Test
    public void testColumnPrimaryComment() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(false).build()
        );
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();

        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("shop_code")).dataType(
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(null).comment(new Comment("comment"))
                .aliasedName(new AliasedName("ali")).build()
        );
        CreateDimTable after = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns2
        ).build();

        List<BaseStatement> statements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, statements.size());
        ChangeCol changeCol = (ChangeCol)statements.get(0);
        assertEquals(changeCol.toString(),
            "ALTER TABLE dim_shop CHANGE COLUMN shop_code shop_code ALIAS 'ali' BIGINT COMMENT 'comment'");
    }

    @Test
    public void testSetProperties() {
        QualifiedName dimShop = QualifiedName.of("dim_shop");
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(dimShop).build();
        CreateDimTable dimTable = CreateDimTable.builder().tableName(dimShop).properties(
            ImmutableList.of(new Property("lifecycle", "10"))).build();
        List<BaseStatement> statements = createTableCompareNode.compareResult(createDimTable, dimTable,
            CompareStrategy.INCREMENTAL);
        BaseStatement baseStatement = statements.get(0);
        assertEquals(baseStatement.toString(), "ALTER TABLE dim_shop SET PROPERTIES('lifecycle'='10')");

    }

    @Test
    public void testCompareAliasName() {
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        CreateDimTable after = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).aliasedName(
            new AliasedName("alias")).build();
        List<BaseStatement> baseStatements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        BaseStatement statement = baseStatements.get(0);
        assertEquals(statement.toString(), "ALTER TABLE dim_shop SET ALIAS 'alias'");
    }

    @Test
    public void testCommentIsNull() {
        CreateDimTable before = getCreateDimTable(null);
        CreateDimTable after = getCreateDimTable(new Comment(null));

        List<BaseStatement> baseStatements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(0, baseStatements.size());

        before = getCreateDimTable(new Comment(""));
        after = getCreateDimTable(new Comment(null));
        baseStatements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(0, baseStatements.size());

        before = getCreateDimTable(new Comment("abc"));
        after = getCreateDimTable(new Comment(null));
        baseStatements = createTableCompareNode.compareResult(before, after,
            CompareStrategy.INCREMENTAL);
        assertEquals(1, baseStatements.size());
    }


    private CreateDimTable getCreateDimTable(Comment comment) {
        CreateDimTable before = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            ImmutableList.of(ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .colName(new Identifier("col1")).comment(comment).build())
        ).build();
        return before;
    }

    @Test
    public void testCreateTableWithFull() {
        List<BaseStatement> baseStatements = createTableCompareNode.compareNode(null, CreateTable.builder().tableName(QualifiedName.of("abc")).build(), CompareStrategy.FULL);
        assertEquals(2, baseStatements.size());
        BaseStatement baseStatement = baseStatements.get(0);
        assertEquals(baseStatement.toString(), "DROP TABLE IF EXISTS abc");
    }
}