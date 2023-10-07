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

package com.aliyun.fastmodel.driver.cli.command;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/22
 */
public class ClientOptionsTest {

    ClientOptions clientOptions;

    @Before
    public void setUp() throws Exception {
        clientOptions = new ClientOptions();
    }

    @Test
    public void testAsProperties() {
        clientOptions.tenantId = "tenant";
        clientOptions.path = "studio/driver";
        clientOptions.accessToken = "accessToken";
        clientOptions.database = "database";
        Properties properties = clientOptions.asProperties();
        assertEquals(properties.get("path"), "studio/driver");
        assertEquals(properties.get("token"), "accessToken");
    }
}