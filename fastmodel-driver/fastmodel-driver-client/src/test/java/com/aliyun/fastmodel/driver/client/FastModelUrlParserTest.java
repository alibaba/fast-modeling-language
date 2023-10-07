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

import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/3
 */
public class FastModelUrlParserTest {

    @Test
    public void parse() throws URISyntaxException {
        Properties parse = FastModelUrlParser.parse("jdbc:fastmodel://mode.engine:1337/dbname",
            new Properties());
        assertEquals("dbname", parse.getProperty("database"));

        parse = FastModelUrlParser.parse("jdbc:fastmodel:tenant://mode.engine:1337/dbname?user=y",
            new Properties());
        assertEquals("dbname", parse.getProperty("database"));
        assertEquals(parse.getProperty("user"), "y");

    }

    @Test
    public void testGetMode() {
        String mode = FastModelUrlParser.getMode("jdbc:fastmodel://mode.engine:1337/dbname");
        assertEquals("", mode);

        mode = FastModelUrlParser.getMode("jdbc:fastmodel:tenant://mode.engine:1337/dbname");
        assertEquals("tenant", mode);

    }
}