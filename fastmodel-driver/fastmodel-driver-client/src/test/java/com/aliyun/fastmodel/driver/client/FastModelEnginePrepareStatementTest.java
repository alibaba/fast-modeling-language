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

import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.command.sample.SampleCommandFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/10
 */
public class FastModelEnginePrepareStatementTest {
    FastModelEngineConnection connection = null;

    SampleCommandFactory sampleCommandFactory = new SampleCommandFactory();

    @Before
    public void setUp() throws Exception {
        Properties info = new Properties();
        info.setProperty("database", "demo");
        ExecuteCommand executeCommand = sampleCommandFactory.createStrategy(
            "sample", info
        );
        connection = new FastModelEngineConnection("jdbc:fastmodel://localhost:8082", executeCommand);

    }

    @Test
    public void testExecuteQuery() throws SQLException {
        FastModelEnginePrepareStatement fastModelEnginePrepareStatement =
            new FastModelEnginePrepareStatement(connection, "show tables like ?", 0);
        fastModelEnginePrepareStatement.setString(1, "%ads%");
        String sql = fastModelEnginePrepareStatement.getSql();
        assertEquals(sql, "show tables like ?");
        String s = fastModelEnginePrepareStatement.buildSql();
        assertEquals("show tables like  '%ads%' ", s);
    }

    @Test
    public void testIsSelect() {
        String sql = "--abc\n select * from abc";
        boolean select = FastModelEngineStatement.isSelect(sql);
        assertTrue(select);

        sql = "create table a.b(a bigint) comment 'abc'";
        select = FastModelEngineStatement.isSelect(sql);
        assertFalse(select);
    }

    @Test
    public void testExecuteUpdate() throws SQLException {
        FastModelEnginePrepareStatement fastModelEnginePrepareStatement =
            new FastModelEnginePrepareStatement(connection, "update abc set b = ?", 0);
        fastModelEnginePrepareStatement.setString(1, "%ads%");
        int i = fastModelEnginePrepareStatement.executeUpdate();
        assertEquals(1, i);
    }
}