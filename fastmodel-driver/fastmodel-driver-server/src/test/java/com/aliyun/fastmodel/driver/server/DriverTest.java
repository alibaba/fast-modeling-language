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

package com.aliyun.fastmodel.driver.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/2
 */
public class DriverTest extends DriverBaseTest {

    String url = "jdbc:fastmodel://localhost:8082/jdbc";

    static {
        try {
            Class.forName("com.aliyun.fastmodel.driver.client.FastModelEngineDriver");
        } catch (ClassNotFoundException e) {
            //throw e
        }
    }

    @Test
    public void testConnection() throws SQLException {
        Properties properties = getProperties();
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show tables;");
        assertTrue(resultSet.next());
        String string = resultSet.getString(1);
        assertEquals(string, "abc");
        assertFalse(resultSet.next());
        connection.close();
    }

    @Test
    public void testPrepare() throws SQLException {
        Properties properties = getProperties();
        Connection connection = DriverManager.getConnection(url, properties);
        PreparedStatement preparedStatement = connection.prepareStatement("show tables from dingtalk LIKE ? ");
        preparedStatement.setString(1, "%ods%");
        ResultSet resultSet = preparedStatement.executeQuery();
        assertNotNull(resultSet);
        connection.close();
    }

    @Test
    public void testExecuteCreateTable() throws SQLException {
        Properties properties = getProperties();
        properties.put("database", "dataworks");
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        boolean execute = statement.execute(
            "create dim table t1 (a bigint, primary key(a)) comment 'comment' with('key'='value')");
        connection.close();
        assertFalse(execute);

    }

    @Test
    public void testExecuteUpdate() throws SQLException {
        Properties properties = getProperties();
        properties.put("database", "dataworks");
        Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
    }

}
