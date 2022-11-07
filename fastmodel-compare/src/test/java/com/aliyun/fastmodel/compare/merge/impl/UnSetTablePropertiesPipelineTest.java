/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
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
public class UnSetTablePropertiesPipelineTest {

    UnSetTablePropertiesPipeline unSetTablePropertiesPipeline ;

    @Before
    public void setUp() throws Exception {
        unSetTablePropertiesPipeline = new UnSetTablePropertiesPipeline();
    }

    @Test
    public void process() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .properties(Lists.newArrayList(
                new Property("c1","abc")
            )).build();
        CreateTable process = unSetTablePropertiesPipeline.process(input,
            new UnSetTableProperties(QualifiedName.of("abc"), Lists.newArrayList("c1")));
        assertEquals(process.getProperties().size(), 0);
    }

    @Test
    public void testProcessNotExist() {
         CreateTable input = CreateTable.builder()
             .detailType(TableDetailType.ADS)
            .properties(Lists.newArrayList(
                new Property("c2","abc")
            )).build();
        CreateTable process = unSetTablePropertiesPipeline.process(input,
            new UnSetTableProperties(QualifiedName.of("abc"), Lists.newArrayList("c1")));
        assertEquals(process.getProperties().size(), 1);

    }
}