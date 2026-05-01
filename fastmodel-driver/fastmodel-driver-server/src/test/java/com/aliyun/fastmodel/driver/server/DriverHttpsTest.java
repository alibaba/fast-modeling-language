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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 突然发现没法本地启动https服务，无法连接上，暂时先ignore
 *
 * @author panguanjing
 * @date 2020/12/2
 */
@Slf4j
@Ignore
public class DriverHttpsTest {

    String url = "jdbc:fastmodel://localhost:8082/";

    static {
        try {
            Class.forName("com.aliyun.fastmodel.driver.client.FastModelEngineDriver");
        } catch (ClassNotFoundException e) {

        }
    }

    private static final SimpleHttpsServer HTTPS_SERVER = new SimpleHttpsServer(8082);

    @BeforeClass
    public static void before() {
        try {
            HTTPS_SERVER.startHttps();
        } catch (Exception e) {
            log.error("StartHttps error", e);
        }
    }

    @AfterClass
    public static void after() {
        HTTPS_SERVER.close();
    }

    @Test
    public void testShowTables() throws SQLException {
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

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("path", "jdbc");
        properties.setProperty("ssl", String.valueOf(true));
        properties.setProperty("sslMode", "none");
        return properties;
    }

}
