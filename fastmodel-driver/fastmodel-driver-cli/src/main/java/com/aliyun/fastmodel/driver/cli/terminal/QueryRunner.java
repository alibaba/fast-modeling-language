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

package com.aliyun.fastmodel.driver.cli.terminal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.aliyun.fastmodel.driver.model.DriverUtil;
import com.aliyun.fastmodel.driver.model.QueryResult;
import lombok.Getter;

/**
 * 查询执行器
 *
 * @author panguanjing
 * @date 2020/12/22
 */
@Getter
public class QueryRunner {

    static {
        try {
            Class.forName("com.aliyun.fastmodel.driver.client.FastModelEngineDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private final Properties properties;

    public QueryRunner(Properties properties) {
        this.properties = properties;
    }

    public Connection getConnection() throws Exception {
        String url = properties.getProperty("url");
        boolean prefix = url.startsWith("jdbc:fastmodel");
        if (prefix) {
            return DriverManager.getConnection(url, properties);
        }
        return DriverManager.getConnection("jdbc:fastmodel://" + url, properties);
    }

    public QueryResult execute(String statement) throws SQLException {
        try (Connection connection = getConnection(); Statement statementObject = connection.createStatement()) {
            if (!DriverUtil.isSelect(statement)) {
                statementObject.executeUpdate(statement);
                return QueryResult.EMPTY;
            }
            ResultSet resultSet = statementObject.executeQuery(statement);
            if (resultSet == null) {
                return QueryResult.EMPTY;
            }
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<DriverColumnInfo> columnInfo = toDriverColumn(metaData);
            List<DriverRow> rows = toRows(resultSet);
            return new QueryResult(
                columnInfo,
                rows
            );
        } catch (Exception e) {
            throw new SQLException("execute statement exception", e);
        }
    }

    private List<DriverRow> toRows(ResultSet resultSet) throws SQLException {
        List<DriverRow> driverRows = new ArrayList<>();
        while (resultSet.next()) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            List<Object> list = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                list.add(resultSet.getObject(i));
            }
            DriverRow driverRow = new DriverRow(list);
            driverRows.add(driverRow);
        }
        return driverRows;
    }

    private List<DriverColumnInfo> toDriverColumn(ResultSetMetaData metaData) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<DriverColumnInfo> driverColumnInfo = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            driverColumnInfo.add(
                new DriverColumnInfo(metaData.getColumnName(i), new DriverDataType(metaData.getColumnType(i))));
        }
        return driverColumnInfo;
    }
}
