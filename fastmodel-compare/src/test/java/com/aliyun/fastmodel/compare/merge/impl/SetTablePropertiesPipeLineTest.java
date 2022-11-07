/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.google.common.collect.ImmutableList;
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
public class SetTablePropertiesPipeLineTest {

    SetTablePropertiesPipeLine setTablePropertiesPipeLine;

    @Before
    public void setUp() throws Exception {
        setTablePropertiesPipeLine = new SetTablePropertiesPipeLine();
    }

    @Test
    public void process() {
        CreateTable input = CreateTable.builder()
            .properties(null)
            .detailType(TableDetailType.ADS)
            .build();
        CreateTable process = setTablePropertiesPipeLine.process(input, new SetTableProperties(QualifiedName.of("abc"), Lists.newArrayList(
            new Property("c1", "v1")
        )));
        List<Property> properties = process.getProperties();
        assertEquals(properties.size(), 1);
        assertEquals(properties.get(0).getName(), "c1");
        assertEquals(properties.get(0).getValue(), "v1");

    }

    @Test
    public void testProcessWithProperty() {
        List<Property> properties = ImmutableList.of(
            new Property("c1", "v2"),
            new Property("c2", "v3")
        );
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .properties(properties)
            .build();
        CreateTable process = setTablePropertiesPipeLine.process(input, new SetTableProperties(QualifiedName.of("abc"), Lists.newArrayList(
            new Property("c1", "v1")
        )));
        assertEquals(process.getProperties().size(), 2);
        assertEquals(process.getProperties().get(0).getValue(), "v1");
    }

    @Test
    public void testProcessWithPropertyMerge() {
        List<Property> properties = ImmutableList.of(
            new Property("c1", "v2"),
            new Property("c2", "v3")
        );
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .properties(properties)
            .build();
        CreateTable process = setTablePropertiesPipeLine.process(input, new SetTableProperties(QualifiedName.of("abc"), Lists.newArrayList(
            new Property("c3", "v3")
        )));
        assertEquals(process.getProperties().size(), 3);
        assertEquals(process.getProperties().get(0).getValue(), "v2");
    }
}