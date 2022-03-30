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

package com.aliyun.fastmodel.driver.client.utils;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.driver.client.FastModelResultSet;
import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.model.DriverRow;
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

public class FastModelResultSetBuilderTest {

    FastModelResultSetBuilder builder;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testEmptyResultSet() throws FastModelException {
        builder = FastModelResultSetBuilder.builder().build();
        FastModelResultSet fastModelResultSet = builder.emptyResultSet();
        assertNotNull(fastModelResultSet.getDriverColumnInfoList());
    }

    @Test
    public void testCreateResultSet() throws FastModelException {
        List<String> columns = new ArrayList<>();
        columns.add("a");
        columns.add("b");
        columns.add("c");
        List<String> types = new ArrayList<>();
        types.add("java.lang.String");
        types.add("java.lang.Long");
        types.add("java.lang.Long");
        List<List<?>> rows = new ArrayList<>();
        List<Object> row = new ArrayList<>();
        row.add("abc");
        row.add(1L);
        row.add(1L);
        rows.add(row);
        builder = FastModelResultSetBuilder.builder().names(columns).types(types).rows(rows).build();
        FastModelResultSet result = builder.createResult();
        List<DriverRow> rows1 = result.getRows();
        assertEquals(1, rows1.size());
    }
}