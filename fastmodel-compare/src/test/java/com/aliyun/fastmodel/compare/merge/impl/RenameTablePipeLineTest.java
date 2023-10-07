/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class RenameTablePipeLineTest {

    RenameTablePipeLine renameTablePipeLine;

    @Before
    public void setUp() throws Exception {
        renameTablePipeLine = new RenameTablePipeLine();
    }

    @Test
    public void testProcess() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.FACTLESS_FACT)
            .tableName(QualifiedName.of("abc")).build();
        CreateTable process = renameTablePipeLine.process(input, new RenameTable(QualifiedName.of("abc"), QualifiedName.of("target")));
        assertEquals(process.getClass(), CreateFactTable.class);
        assertEquals(process.getQualifiedName(), QualifiedName.of("target"));
    }
}