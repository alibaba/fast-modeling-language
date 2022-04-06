/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class HiveViewVisitorTest {

    @Test
    public void toSampleExpression() {
        HiveTransformContext context = HiveTransformContext.builder()
            .database("db")
            .schema("sc")
            .build();
        HiveViewVisitor hiveViewVisitor = new HiveViewVisitor(context);
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .colName(new Identifier("a"))
                .comment(new Comment("abc"))
                .build()
        );
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("t1"))
            .columns(columns)
            .build();
        hiveViewVisitor.visitCreateTable(table, 0);
        String string = hiveViewVisitor.getBuilder().toString();
        assertEquals(string, "CREATE OR REPLACE VIEW db.sc.t1\n"
            + "(\n"
            + "   a COMMENT 'abc'\n"
            + ")\n"
            + "AS SELECT ''\n"
            + "\n");
    }
}