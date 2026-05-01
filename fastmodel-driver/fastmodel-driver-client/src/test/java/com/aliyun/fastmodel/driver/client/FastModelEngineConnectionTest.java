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
import java.util.Properties;

import com.aliyun.fastmodel.driver.client.command.CommandFactory;
import com.aliyun.fastmodel.driver.client.command.CommandType;
import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/22
 */

public class FastModelEngineConnectionTest {

    FastModelEngineConnection fastModelEngineConnection;

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("ssl", "true");
        ExecuteCommand command = CommandFactory.createStrategy(
            CommandType.TENANT,
            properties
        );
        fastModelEngineConnection = new FastModelEngineConnection("jdbc:fastmodel://localhost:7071/database",
            command);
    }

    @Test
    public void testValid() throws SQLException {
        boolean valid = fastModelEngineConnection.isValid(100);
        assertTrue(valid);
    }

    @Test
    public void testAbort() throws SQLException {
        fastModelEngineConnection.abort(null);
    }

    @Test
    public void testCatalog() throws SQLException {
        String catalog = fastModelEngineConnection.getCatalog();
        assertNull(catalog);
    }
}