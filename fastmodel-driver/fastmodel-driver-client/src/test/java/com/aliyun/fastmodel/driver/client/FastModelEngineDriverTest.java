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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.aliyun.fastmodel.driver.client.exception.IllegalConfigException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/4
 */
public class FastModelEngineDriverTest {
    FastModelEngineDriver fastModelEngineDriver = new FastModelEngineDriver();

    @Test(expected = IllegalConfigException.class)
    public void testConnectionWithNull() throws SQLException {
        Connection connect = fastModelEngineDriver.connect("jdbc:abc://localhost:8080", null);
        assertNull(connect);
    }

    @Test
    public void testAccepts() throws SQLException {
        boolean b = fastModelEngineDriver.acceptsURL("jdbc:fastmodel://localhost:8080");
        assertTrue(b);
    }

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Properties info = new Properties();
        info.setProperty("host", "www.google.com");
        info.setProperty("port", "18000");
        DriverPropertyInfo[] propertyInfo = fastModelEngineDriver.getPropertyInfo(
            "jdbc:fastmodel://localhost:8082/dataworks",
            info);
        for (DriverPropertyInfo d : propertyInfo) {
            if (d.name.equalsIgnoreCase("host")) {
                assertEquals(d.value, "www.google.com");
            }
        }
    }
}