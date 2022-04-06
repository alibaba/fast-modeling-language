/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.builder;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class ViewBuilderTest {
    ViewBuilder createViewBuilder = new ViewBuilder();

    @Test
    public void testBuild() {
        List<ColumnDefinition> columnDefinitions = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c3"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.TIMESTAMP))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c4"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BOOLEAN))
                .build()
        );
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columnDefinitions)
            .comment(new Comment("abc"))
            .build();
        DialectNode build = createViewBuilder.build(source, MaxComputeContext.builder().
            transformToView(ViewSetting.builder().transformToView(true).build()).appendSemicolon(true).build());
        assertEquals(build.getNode(), "CREATE OR REPLACE VIEW abc\n"
            + "(\n"
            + "   c1,\n"
            + "   c2,\n"
            + "   c3,\n"
            + "   c4\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "AS SELECT\n"
            + "  0\n"
            + ", ''\n"
            + ", GETDATE()\n"
            + ", true\n"
            + "\n"
            + ";");
    }

    @Test
    public void testBuildDateDecimal() {
        List<ColumnDefinition> columnDefinitions = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DECIMAL))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE))
                .build()
        );
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columnDefinitions)
            .comment(new Comment("abc"))
            .build();
        DialectNode build = createViewBuilder.build(source, MaxComputeContext.builder().
            transformToView(ViewSetting.builder().transformToView(true).build()).appendSemicolon(true).build());
        assertEquals(build.getNode(), "CREATE OR REPLACE VIEW abc\n"
            + "(\n"
            + "   c1,\n"
            + "   c2\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "AS SELECT\n"
            + "  0.0\n"
            + ", GETDATE()\n"
            + "\n"
            + ";");
    }

    @Test
    public void testColumnEmpty() {
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .build();
        DialectNode build = createViewBuilder.build(source, MaxComputeContext.builder().
            transformToView(ViewSetting.builder().transformToView(true).build()).appendSemicolon(true).build());
        assertFalse(build.isExecutable());
    }

    @Test
    public void testBuildWithPartition() {
        List<ColumnDefinition> columnDefinitions = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DECIMAL))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE))
                .build()
        );
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columnDefinitions)
            .comment(new Comment("abc"))
            .partition(new PartitionedBy(
                Lists.newArrayList(
                    ColumnDefinition.builder()
                        .colName(new Identifier("abc"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .build()
                )
            ))
            .build();
        DialectNode build = createViewBuilder.build(source, MaxComputeContext.builder().
            transformToView(ViewSetting.builder().transformToView(true).build()).appendSemicolon(true).build());
        assertEquals(build.getNode(), "CREATE OR REPLACE VIEW abc\n"
            + "(\n"
            + "   c1 ,\n"
            + "   c2 ,\n"
            + "   abc\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "AS SELECT\n"
            + "  0.0\n"
            + ", GETDATE()\n"
            + ", 0\n"
            + "\n"
            + ";");
    }

    @Test
    public void testBuilderFactory() {
        BaseStatement source = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, DialectMeta.getMaxCompute(), TransformContext.builder().
            transformToView(ViewSetting.builder().transformToView(true).build()).build());
        assertEquals(builder.getClass(), ViewBuilder.class);

        builder = BuilderFactory.getInstance().getBuilder(source, DialectMeta.getMaxCompute(), TransformContext.builder().
            transformToView(ViewSetting.builder().transformToView(false).build()).build());
        assertEquals(builder.getClass(), DefaultBuilder.class);
    }
}
