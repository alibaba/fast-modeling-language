/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class DropColPipelineTest {

    DropColPipeline dropColPipeline = new DropColPipeline();

    @Test
    public void process() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .build()
            )).build();
        DropCol baseStatement = new DropCol(
            QualifiedName.of("abc"),
            new Identifier("c1")
        );
        CreateTable process = dropColPipeline.process(input, baseStatement);
        assertEquals(process.getColumnDefines().size(), 0);
    }
}