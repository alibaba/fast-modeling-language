/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.ArrayList;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
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
public class DropConstraintPipelineTest {

    DropConstraintPipeline dropConstraintPipeline;

    @Before
    public void setUp() throws Exception {
        dropConstraintPipeline = new DropConstraintPipeline();
    }

    @Test
    public void testProcess() {
        ArrayList<BaseConstraint> c2 = Lists.newArrayList(new PrimaryConstraint(new Identifier("C1"), Lists.newArrayList()));
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .constraints(c2)
            .build();
        CreateTable process = dropConstraintPipeline.process(input, new DropConstraint(QualifiedName.of("abc"), new Identifier("c1")));
        assertEquals(process.getConstraintStatements().size(), 0);
    }
}