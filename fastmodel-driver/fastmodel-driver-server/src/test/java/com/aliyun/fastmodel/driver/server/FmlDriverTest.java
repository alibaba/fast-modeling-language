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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 用于FmlDriver Test
 *
 * @author panguanjing
 * @date 2020/12/15
 */
@Slf4j
@Ignore
public class FmlDriverTest {

    public static final int PORT = 80;
    public static final String LOCALHOST = "model-engine-v1.daily-ms.dw.alibaba-inc.com";

    static {
        try {
            Class.forName("com.aliyun.fastmodel.driver.client.FastModelEngineDriver");
        } catch (ClassNotFoundException e) {
            log.error("can't find the driver", e);
        }
    }

    @Before
    public void before() {

    }

    @Test
    public void testFmlDriver() throws SQLException {
        Connection connection = getConnection(LOCALHOST, PORT);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show tables; ");
        while (resultSet.next()) {
            String code = resultSet.getString(1);
            assertEquals(code, "dim_shop");
        }
        statement.close();
        connection.close();
    }

    private Connection getConnection(String host, Integer port) throws SQLException {
        Properties info = new Properties();
        info.setProperty("ssl", "false");
        info.setProperty("token", "96063edcb9f5d80926d14f70e491ff1b73463f70ddc2773a843bc98e4e4ca36f");
        info.setProperty("baseKey", "base_dp");
        info.setProperty("tenantId", "1");
        return DriverManager.getConnection("jdbc:fastmodel://" + host + ":" + port + "/autotest", info);
    }

    @Test
    public void testShowCreateTable() throws SQLException {
        Connection connection = getConnection(LOCALHOST, PORT);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show create table dim_shop;");
        while (resultSet.next()) {
            String code = resultSet.getString(1);
            assertEquals(code, "dim_shop");
        }
        statement.close();
        connection.close();
    }

    @Test
    public void testDDL() throws SQLException {
        Connection connection = getConnection(LOCALHOST, PORT);
        Statement statement = connection.createStatement();
        boolean execute = statement.execute("alter table dim_shop rename to dim_shop_2");
        close(statement, connection);
        assertTrue(execute);
    }

    private void close(Statement statement, Connection connection) throws SQLException {
        statement.close();
        connection.close();

    }

    @Test
    public void testCreateDim() throws SQLException {
        String sql = "create dim table dim_item (a bigint primary key, b varchar(2) not null) comment '维度表'";
        assertFalse(executeStatement(sql));
    }

    @Test
    public void testSetComment() throws SQLException {
        boolean result = executeStatement("alter table dim_shop set comment 'newShopName'");
        assertTrue(result);
    }

    @Test
    public void testSetProperties() throws SQLException {
        boolean result = executeStatement("alter table dim_shop set tblproperties('key1'='value1')");
        assertFalse(result);
    }

    @Test
    public void testShowObjectsWithLike() throws SQLException {
        boolean result = executeStatement("show tables like 'abc%'");
        assertTrue(result);
    }

    @Test
    public void testDescribe() throws SQLException {
        Connection connection = getConnection("localhost", PORT);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("desc table dim_shop");
        assertTrue(resultSet.next());
    }

    private boolean executeStatement(String sql) throws SQLException {
        Connection connection = getConnection("localhost", PORT);
        Statement statement = connection.createStatement();
        try {
            return statement.execute(sql);
        } finally {
            close(statement, connection);
        }
    }

}
