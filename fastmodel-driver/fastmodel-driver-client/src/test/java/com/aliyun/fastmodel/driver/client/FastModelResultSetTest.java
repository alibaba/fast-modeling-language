/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.driver.client;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;
import com.aliyun.fastmodel.driver.model.DriverResult;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.aliyun.fastmodel.driver.model.QueryResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/4
 */
@RunWith(MockitoJUnitRunner.class)
public class FastModelResultSetTest {

    FastModelResultSet resultSet = null;

    @Mock
    FastModelEngineStatement statement;

    @Before
    public void before() throws FastModelException {
        JSONObject jsonObject = new JSONObject();
        List<DriverColumnInfo> columnInfos = new ArrayList<>();
        columnInfos = prepareColumn();
        List<DriverRow> rows = new ArrayList<>();
        List<Object> rowData = new ArrayList<>();
        rowData.add(1);
        DriverRow driverRow = new DriverRow(rowData);
        rows.add(driverRow);
        QueryResult data = new QueryResult(
            columnInfos,
            rows
        );
        jsonObject.fluentPut("data", new DriverResult<QueryResult>(data, true, null));
        resultSet = new FastModelResultSet(jsonObject, statement);

    }

    @Test
    public void testGetBoolean() throws SQLException {
        resultSet.next();
        boolean aBoolean = resultSet.getBoolean(1);
        assertFalse(aBoolean);
    }

    @Test
    public void testGetByte() throws SQLException {
        resultSet.next();
        byte aByte = resultSet.getByte(1);
        assertEquals((byte)1, aByte);
    }

    @Test
    public void testGetString() throws SQLException {
        resultSet.next();
        String string = resultSet.getString(1);
        assertNotNull(string);
    }

    @Test
    public void testGetInt() throws SQLException {
        resultSet.next();
        int anInt = resultSet.getInt(1);
        assertTrue(anInt > 0);
    }

    private List<DriverColumnInfo> prepareColumn() {
        DriverColumnInfo driverColumnInfo = new DriverColumnInfo();
        driverColumnInfo.setColumnName("col1");
        driverColumnInfo.setDataType(new DriverDataType(Types.INTEGER));
        return Collections.singletonList(driverColumnInfo);
    }

    @Test
    public void getObject() throws SQLException {
        resultSet.next();
        Integer object = resultSet.getObject(1, Integer.class);
        assertNotNull(object);
        assertEquals(object, new Integer(1));
    }

}