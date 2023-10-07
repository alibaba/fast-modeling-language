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

import java.sql.SQLException;
import java.util.Properties;

import com.aliyun.fastmodel.driver.model.QueryResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/4
 */
public class QueryRunnerTest {
    QueryRunner queryRunner = null;

    @Before
    public void setUp() throws Exception {
        Properties properties = prepareProperties();
        queryRunner= new QueryRunner(properties);
    }

    @Test(expected = Exception.class)
    public void testExecute() throws SQLException {
        QueryResult execute = queryRunner.execute("show tables;");
        assertNotNull(execute);
    }

    private Properties prepareProperties() {
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:fastmodel://localhost:8082/dataworks");
        return properties;
    }
}