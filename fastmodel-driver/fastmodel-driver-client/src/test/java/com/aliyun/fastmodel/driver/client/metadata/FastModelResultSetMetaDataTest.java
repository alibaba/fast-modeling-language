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

package com.aliyun.fastmodel.driver.client.metadata;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.driver.client.FastModelResultSet;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/4
 */
@RunWith(MockitoJUnitRunner.class)
public class FastModelResultSetMetaDataTest {
    FastModelResultSetMetaData resultSetMetaData = null;
    @Mock
    private FastModelResultSet resultSet;

    @Before
    public void setUp() throws Exception {
        resultSetMetaData = new FastModelResultSetMetaData(resultSet);
    }

    @Test
    public void getColumnCount() throws SQLException {
        prepareColumn();
        int columnCount = resultSetMetaData.getColumnCount();
        assertEquals(1, columnCount);
    }

    @Test
    public void getColumnTypeName() throws SQLException {
        prepareColumn();
        String columnTypeName = resultSetMetaData.getColumnTypeName(1);
        assertEquals(columnTypeName, Boolean.class.getName());
    }

    @Test
    public void getColumnType() throws SQLException {
        prepareColumn();
        int columnType = resultSetMetaData.getColumnType(1);
        assertEquals(columnType, Types.BOOLEAN);
    }

    private void prepareColumn() {
        List<DriverColumnInfo> columnInfo = new ArrayList<>();
        DriverColumnInfo info = new DriverColumnInfo(
            "driverName",
            new DriverDataType(Types.BOOLEAN)
        );
        columnInfo.add(info);
        given(resultSet.getDriverColumnInfoList()).willReturn(columnInfo);
    }

    @Test
    public void isAutoCommit() throws SQLException {
        boolean autoIncrement = resultSetMetaData.isAutoIncrement(1);
        assertFalse(autoIncrement);
    }

    @Test
    public void isCaseSensitive() throws SQLException {
        boolean caseSensitive = resultSetMetaData.isCaseSensitive(1);
        assertTrue(caseSensitive);
    }

    @Test
    public void isSearchable() throws SQLException {
        boolean searchable = resultSetMetaData.isSearchable(1);
        assertTrue(searchable);
    }

    @Test
    public void isCurrency() throws SQLException {
        assertFalse(resultSetMetaData.isCurrency(1));
    }

    @Test
    public void isNullable() throws SQLException {
        assertEquals(1, resultSetMetaData.isNullable(1));
    }

    @Test
    public void isSigned() throws SQLException {
        boolean signed = resultSetMetaData.isSigned(1);
        assertFalse(signed);
    }

    @Test
    public void getColumnDisplaySize() throws SQLException {
        assertEquals(80, resultSetMetaData.getColumnDisplaySize(1));
    }

    @Test
    public void getColumnLabel() throws SQLException {
        prepareColumn();
        String columnLabel = resultSetMetaData.getColumnLabel(1);
        assertNotNull(columnLabel);
        assertEquals("driverName", columnLabel);
    }

    @Test
    public void getColumnName() throws SQLException {
        prepareColumn();
        String colName = resultSetMetaData.getColumnName(1);
        assertEquals("driverName", colName);
    }

    @Test
    public void getSchemaName() throws SQLException {
        String schemaName = resultSetMetaData.getSchemaName(1);
        assertEquals("", schemaName);
    }

    @Test
    public void getPrecision() throws SQLException {
        int precision = resultSetMetaData.getPrecision(1);
        assertEquals(0, precision);
    }

    @Test
    public void getScale() throws SQLException {
        int scale = resultSetMetaData.getScale(1);
        assertEquals(0, scale);
    }

    @Test
    public void getTableName() throws SQLException {
        String tableName = resultSetMetaData.getTableName(1);
        assertEquals("", tableName);

    }

    @Test
    public void getCatalogName() throws SQLException {
        String catalogName = resultSetMetaData.getCatalogName(1);
        assertNull(catalogName);
    }

    @Test
    public void isReadOnly() throws SQLException {
        boolean readOnly = resultSetMetaData.isReadOnly(1);
        assertTrue(readOnly);
    }

    @Test
    public void isDefinitelyWritable() throws SQLException {
        boolean definitelyWritable = resultSetMetaData.isDefinitelyWritable(1);
        assertFalse(definitelyWritable);
    }

    @Test
    public void getColumnClassName() throws SQLException {
        prepareColumn();
        String columnClassName = resultSetMetaData.getColumnClassName(1);
        assertEquals(columnClassName, "java.lang.Boolean");
    }

}