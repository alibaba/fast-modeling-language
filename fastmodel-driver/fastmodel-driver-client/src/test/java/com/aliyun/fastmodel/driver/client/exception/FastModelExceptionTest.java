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

package com.aliyun.fastmodel.driver.client.exception;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/5
 */
public class FastModelExceptionTest {

    @Test
    public void testException() {
        FastModelException fastModelException = new FastModelException(
            new SQLException(),
            "localhost"
        );
        assertTrue(fastModelException.getMessage().contains("Exception Context"));
    }

    static {
        try {
            Class.forName("com.aliyun.fastmodel.driver.client.FastModelEngineDriver");
        } catch (ClassNotFoundException e) {

        }
    }

    @Test(expected = SQLException.class)
    public void testNullPointException() throws SQLException {
        String url = "jdbc:fastmodel://model-engine,/autotest";
        Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        statement.executeQuery("show tables");
        assertNotNull(statement);
    }
}