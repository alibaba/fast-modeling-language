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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.aliyun.fastmodel.driver.client.FastModelResultSet;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;

/**
 * result set meta data
 *
 * @author panguanjing
 * @date 2020/12/9
 */
public class FastModelResultSetMetaData implements ResultSetMetaData {

    private final FastModelResultSet resultSet;

    public FastModelResultSetMetaData(FastModelResultSet fastModelResultSet) {
        resultSet = fastModelResultSet;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getDriverColumnInfoList().size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return resultSet.getDb();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        DriverColumnInfo driverColumnInfo = resultSet.getDriverColumnInfoList().get(column - 1);
        if (driverColumnInfo.getDataType() != null) {
            return driverColumnInfo.getDataType().getType();
        } else {
            return Types.VARCHAR;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumnClassName(column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        DriverColumnInfo driverColumnInfo = getColumn(column);
        DriverDataType dataType = driverColumnInfo.getDataType();
        if (dataType != null) {
            return dataType.getJavaType().getName();
        }
        return String.class.getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T)this;
        }
        throw new SQLException("unable to unwrap to " + iface.toString());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    private DriverColumnInfo getColumn(int column) {
        return resultSet.getDriverColumnInfoList().get(column - 1);
    }
}
