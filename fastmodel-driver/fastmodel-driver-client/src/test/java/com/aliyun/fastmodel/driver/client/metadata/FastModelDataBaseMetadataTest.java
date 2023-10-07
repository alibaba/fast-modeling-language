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

package com.aliyun.fastmodel.driver.client.metadata;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/22
 */
public class FastModelDataBaseMetadataTest {

    FastModelDataBaseMetadata fastModelDataBaseMetadata;

    @Before
    public void setUp() throws Exception {
        fastModelDataBaseMetadata = new FastModelDataBaseMetadata("jdbc:fastmodel://localhost:7070", null);
    }

    @Test
    public void testGetUrl() throws SQLException {
        String url = fastModelDataBaseMetadata.getURL();
        assertNotNull(url);
    }

    @Test
    public void testGetDriverName() throws SQLException {
        String driverName = fastModelDataBaseMetadata.getDriverName();
        assertEquals(driverName, FastModelDataBaseMetadata.FASTMODEL_JDBC);
    }
}