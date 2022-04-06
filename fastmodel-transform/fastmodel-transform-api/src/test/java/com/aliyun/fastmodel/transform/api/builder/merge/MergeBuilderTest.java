/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.builder.merge;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.builder.merge.impl.CreateTableMergeBuilder;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * MergeBuilder
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class MergeBuilderTest {
    MergeBuilder mergeBuilder = new CreateTableMergeBuilder();

    @Test
    public void testGetMainStatement() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .build()
        );
        BaseStatement mainStatement = mergeBuilder.getMainStatement(statements);
        assertNotNull(mainStatement);
    }
}