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

package com.aliyun.fastmodel.driver.server.druid;

import java.sql.SQLException;
import java.util.Properties;

import com.aliyun.fastmodel.driver.server.DriverBaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/9
 */
public class DruidDemoTest extends DriverBaseTest {

    DruidDemo druidDemo = new DruidDemo();

    @Before
    public void setUp() throws Exception {
        Properties properties = getProperties();
        druidDemo.init("jdbc:fastmodel://localhost:8082/jdbc", "com.aliyun.fastmodel.driver.client.FastModelEngineDriver",
            properties);
    }


    @Test
    public void testStart() throws SQLException {
        druidDemo.execute("show tables;");
    }

    @After
    public void tearDown() throws Exception {
        druidDemo.close();
    }
}