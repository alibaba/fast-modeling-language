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
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class AddConstraintPipelineTest {

    AddConstraintPipeline addConstraintPipeline;

    @Before
    public void setUp() throws Exception {
        addConstraintPipeline = new AddConstraintPipeline();
    }

    @Test
    public void process() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .constraints(null)
            .columns(Lists.newArrayList(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .build()
            ))
            .build();
        AddConstraint baseStatment = new AddConstraint(
            QualifiedName.of("abc"),
            new PrimaryConstraint(new Identifier("c1"), Lists.newArrayList(new Identifier("c1")))
        );

        CreateTable process = addConstraintPipeline.process(input, baseStatment);
        assertEquals(process.getClass(), CreateAdsTable.class);
        assertEquals(process.getConstraintStatements().size(), 1);
    }
}