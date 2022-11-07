/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.builder;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveDataTypeName;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveGenericDataType;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class ViewBuilderTest {

    @Test
    public void build() {
        ViewBuilder viewBuilder = new ViewBuilder();
        BaseStatement source = new DropTable(QualifiedName.of("abc"));
        DialectNode build = viewBuilder.build(source, HiveTransformContext.builder().transformToView(
            ViewSetting.builder()
                .transformToView(true)
                .build()
        ).build());
        assertEquals(build.getNode(), "DROP VIEW abc");
    }

    @Test
    public void testCreateView() {
        ViewBuilder viewBuilder = new ViewBuilder();
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(new HiveGenericDataType(HiveDataTypeName.DATE))
                .comment(new Comment("comment1"))
                .build()
        );
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("v_1"))
            .columns(columns)
            .build();
        DialectNode build = viewBuilder.build(source, HiveTransformContext.builder().transformToView(
            ViewSetting.builder()
                .transformToView(true)
                .build()
        ).build());
        assertEquals(build.getNode(), "CREATE OR REPLACE VIEW v_1\n"
            + "(\n"
            + "   c1 COMMENT 'comment1'\n"
            + ")\n"
            + "AS SELECT current_date()\n"
            + "\n");
    }
}