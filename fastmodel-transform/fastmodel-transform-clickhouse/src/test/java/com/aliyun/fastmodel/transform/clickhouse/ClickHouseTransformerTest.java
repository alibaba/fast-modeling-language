/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * ClickHouseTransformerTest
 *
 * @author panguanjing
 * @date 2022/8/10
 */
public class ClickHouseTransformerTest {

    ClickHouseTransformer clickHouseTransformer = new ClickHouseTransformer();

    @Test
    public void reverse() {
        DialectNode node = new DialectNode("create table a (a UInt64 )");
        BaseStatement reverse = clickHouseTransformer.reverse(node);
        CreateTable createTable = (CreateTable)reverse;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(1, columnDefines.size());
        BaseDataType dataType = columnDefines.get(0).getDataType();
        assertEquals(dataType.toString(), "UInt64");
    }
}