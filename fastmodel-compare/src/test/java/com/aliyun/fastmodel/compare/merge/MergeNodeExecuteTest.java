/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.compare.MergeNodeExecute;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * MergeNodeExecuteTest
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class MergeNodeExecuteTest {

    @Test
    public void testSimpleMerge() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .detailType(TableDetailType.ADS)
            .build();
        List<BaseStatement> statements = Lists.newArrayList(
            new SetTableComment(QualifiedName.of("abc"), new Comment("abc"))
        );
        CreateTable merge = MergeNodeExecute.getInstance().merge(createTable, statements);
        assertEquals(merge.getCommentValue(), "abc");
    }

    @Test
    public void testMerge() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .detailType(TableDetailType.ADS)
            .build();
        List<BaseStatement> statements = Lists.newArrayList(
            new SetTableComment(QualifiedName.of("abc"), new Comment("abc")),
            new AddCols(QualifiedName.of("abc"), Lists.newArrayList(ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .build())),
            new SetTableAliasedName(QualifiedName.of("abc"), new AliasedName("alias")),
            new SetTableProperties(QualifiedName.of("abc"), Lists.newArrayList(
                new Property("a", "b"),
                new Property("d", "e")
            ))
        );
        CreateTable merge = MergeNodeExecute.getInstance().merge(createTable, statements);
        assertEquals(merge.getClass(), CreateAdsTable.class);
        assertEquals(merge.getCommentValue(), "abc");
        assertEquals(merge.getColumnDefines().size(), 1);
        assertEquals(merge.getAliasedNameValue(), "alias");
        assertEquals(merge.getProperties().size(), 2);
    }

    @Test
    public void testMergeConstraintAndDropConstraint() {
        CreateTable createTable = CreateTable.builder()
            .detailType(TableDetailType.ENUM_DIM)
            .tableName(QualifiedName.of("abc"))
            .build();
        List<BaseStatement> statements = Lists.newArrayList(
            new AddCols(QualifiedName.of("abc"), Lists.newArrayList(ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .build())),
            new AddConstraint(QualifiedName.of("abc"), new PrimaryConstraint(new Identifier("c1"), Lists.newArrayList(new Identifier("c1"))))
        );
        CreateTable merge = MergeNodeExecute.getInstance().merge(createTable, statements);
        assertEquals(merge.getClass(), CreateDimTable.class);
        assertEquals(merge.getColumnDefines().size(), 1);
        assertEquals(merge.getConstraintStatements().size(), 1);
        merge = MergeNodeExecute.getInstance().merge(merge, Lists.newArrayList(
            new DropConstraint(QualifiedName.of("abc"), new Identifier("c1"))
        ));
        assertEquals(merge.getConstraintStatements().size(), 0);
    }

    @Test
    public void testMergeChangeCol() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .detailType(TableDetailType.ADS)
            .build();
        List<BaseStatement> statements = Lists.newArrayList(
            new AddCols(QualifiedName.of("abc"), Lists.newArrayList(ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .build()))
        );
        CreateTable merge = MergeNodeExecute.getInstance().merge(createTable, statements);
        assertEquals(merge.getColumnDefines().size(), 1);
        merge = MergeNodeExecute.getInstance().merge(merge, Lists.newArrayList(
            new ChangeCol(QualifiedName.of("abc"), new Identifier("c1"), ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .comment(new Comment("abc"))
                .category(ColumnCategory.ATTRIBUTE)
                .defaultValue(new StringLiteral("abc"))
                .notNull(true)
                .primary(true)
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .refDimension(QualifiedName.of("dimension"))
                .refIndicators(Lists.newArrayList(new Identifier("c2")))
                .build())
        ));
        ColumnDefinition columnDefinition = merge.getColumnDefines().get(0);
        assertEquals(columnDefinition.getCommentValue(), "abc");
        assertEquals(columnDefinition.getNotNull(), true);
        assertEquals(columnDefinition.getPrimary(), true);
        assertEquals(columnDefinition.getDefaultValue(), new StringLiteral("abc"));
        assertEquals(columnDefinition.getDataType(), DataTypeUtil.simpleType(DataTypeEnums.BIGINT));
        assertEquals(columnDefinition.getRefDimension(), QualifiedName.of("dimension"));
        assertEquals(columnDefinition.getRefIndicators(), Lists.newArrayList(new Identifier("c2")));
    }

    @Test
    public void testColumnOrder() {
        //b, a, c
        List<ColumnDefinition> columns = Lists.newArrayList(
            getColumn("b"),
            getColumn("a"),
            getColumn("c")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(columns)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("a"),
            getColumn("c"),
            getColumn("b")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));

    }

    @Test
    public void testColumnOrderSeven() {
        //b, a, c
        List<ColumnDefinition> columns = Lists.newArrayList(
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d"),
            getColumn("e"),
            getColumn("f"),
            getColumn("g"),
            getColumn("h")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(columns)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
    }

    @Test
    public void testColumnOrderSeven5() {
        //b, a, c
        List<ColumnDefinition> columns = Lists.newArrayList(
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d"),
            getColumn("e"),
            getColumn("f"),
            getColumn("g"),
            getColumn("h")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(columns)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("g"),
            getColumn("h"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("a"),
            getColumn("c"),
            getColumn("b")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
    }

    @Test
    public void testColumnOrderSeven3() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b

        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d"),
            getColumn("e"),
            getColumn("f"),
            getColumn("g"),
            getColumn("h")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
    }

    @Test
    public void testColumnOrderSeven4() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("d"),
            getColumn("h"),
            getColumn("a"),
            getColumn("g"),
            getColumn("e"),
            getColumn("c"),
            getColumn("b"),
            getColumn("f")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("f"),
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d"),
            getColumn("e"),
            getColumn("g"),
            getColumn("h")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
    }

    @Test
    public void testColumnOrderSeven6() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d"),
            getColumn("e")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
    }


    @Test
    public void testColumnOrderSeven7() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("d"),
            getColumn("c"),
            getColumn("e"),
            getColumn("b"),
            getColumn("a")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("h"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("g"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("f"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
        columnDefinition = columnDefines.get(4);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(5);
        assertEquals(columnDefinition.getColName(), new Identifier("e"));
        columnDefinition = columnDefines.get(6);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(7);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
    }


    @Test
    public void testColumnOrderSeven9() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("b"),
            getColumn("a"),
            getColumn("c"),
            getColumn("d"),
            getColumn("h"),
            getColumn("g")
        );

        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("c"),
            getColumn("d"),
            getColumn("b"),
            getColumn("a")
        );

        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();

        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        String collect = columnDefines.stream().map(x -> {
            return x.getColName().getValue();
        }).collect(Collectors.joining(","));
        assertEquals(collect, "h,g,c,d,b,a");
    }

    @Test
    public void testColumnOrderSeven8() {
        //b, a, c
        List<ColumnDefinition> beforeColumn = Lists.newArrayList(
            getColumn("b"),
            getColumn("a"),
            getColumn("d"),
            getColumn("c"),
            getColumn("f"),
            getColumn("e"),
            getColumn("h"),
            getColumn("g")
        );

        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("h"),
            getColumn("g"),
            getColumn("f"),
            getColumn("e"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b"),
            getColumn("a")
        );

        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(beforeColumn)
            .detailType(TableDetailType.ADS)
            .build();


        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        String collect = columnDefines.stream().map(x -> {
            return x.getColName().getValue();
        }).collect(Collectors.joining(","));
        assertEquals("h,g,f,e,d,c,b,a", collect);

    }

    @Test
    public void testColumnOrderSeven2() {
        //b, a, c
        List<ColumnDefinition> columns = Lists.newArrayList(
            getColumn("a"),
            getColumn("d"),
            getColumn("c"),
            getColumn("b")
        );
        QualifiedName table = QualifiedName.of("abc");
        CreateTable before = CreateTable.builder()
            .tableName(table)
            .columns(columns)
            .detailType(TableDetailType.ADS)
            .build();

        //a, c, b
        List<ColumnDefinition> afterColumns = Lists.newArrayList(
            getColumn("a"),
            getColumn("b"),
            getColumn("c"),
            getColumn("d")
        );
        CreateTable after = CreateTable.builder()
            .tableName(table)
            .columns(afterColumns)
            .detailType(TableDetailType.ADS)
            .build();

        List<BaseStatement> compare = CompareNodeExecute.getInstance().compare(before, after, CompareStrategy.INCREMENTAL);
        CreateTable merge = MergeNodeExecute.getInstance().merge(before, compare);
        List<ColumnDefinition> columnDefines = merge.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("a"));
        columnDefinition = columnDefines.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("b"));
        columnDefinition = columnDefines.get(2);
        assertEquals(columnDefinition.getColName(), new Identifier("c"));
        columnDefinition = columnDefines.get(3);
        assertEquals(columnDefinition.getColName(), new Identifier("d"));
    }

    private SetColumnOrder getSetColumnOrder(QualifiedName table, Identifier old, Identifier before) {
        return new SetColumnOrder(
            table, old, old,
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            before,
            false
        );
    }

    private static ColumnDefinition getColumn(String colName) {
        return ColumnDefinition.builder().colName(new Identifier(colName))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
    }

    @Test(expected = NullPointerException.class)
    public void testMergeNull() {
        MergeNodeExecute.getInstance().merge(null, null);
    }

    @Test
    public void testMergeEmptyStatement() {
        CreateTable merge = MergeNodeExecute.getInstance().merge(CreateTable.builder().build(), null);
        assertNotNull(merge);
    }
}