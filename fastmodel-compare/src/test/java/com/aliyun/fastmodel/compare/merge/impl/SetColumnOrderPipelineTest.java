/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class SetColumnOrderPipelineTest {

    SetColumnOrderPipeline setColumnOrderPipeline = new SetColumnOrderPipeline();

    @Test
    public void process() {
        Identifier c1 = new Identifier("c1");
        Identifier b1 = new Identifier("b1");
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .tableName(QualifiedName.of("abc"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(c1)
                    .build(),
                ColumnDefinition.builder()
                    .colName(b1)
                    .build()
            ))
            .build();
        CreateTable process = setColumnOrderPipeline.process(input, new SetColumnOrder(
            QualifiedName.of("abc"),
            new Identifier("c1"),
            new Identifier("c1"),
            null,
            new Identifier("b1"),
            false
        ));
        assertEquals(process.getClass(), CreateAdsTable.class);
        assertEquals(process.getColumnDefines().get(0).getColName(), new Identifier("b1"));
    }

    @Test
    public void testFirst() {
        Identifier c1 = new Identifier("c1");
        Identifier b1 = new Identifier("b1");
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .tableName(QualifiedName.of("abc"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(b1)
                    .build(),
                ColumnDefinition.builder()
                    .colName(c1)
                    .build()
            ))
            .build();
        CreateTable process = setColumnOrderPipeline.process(input, new SetColumnOrder(
            QualifiedName.of("abc"),
            new Identifier("c1"),
            new Identifier("c1"),
            null,
            null,
            true
        ));
        assertEquals(process.getColumnDefines().size(), 2);
        assertEquals(process.getColumnDefines().get(0).getColName(), new Identifier("c1"));
    }

    @Test
    public void testListAdd() {
        List<String> list = Lists.newArrayList("a", "b", "c");
        list.add(0, "d");
        assertEquals(4, list.size());
    }
}