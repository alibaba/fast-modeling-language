/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LogicalPartitionTest {
    HologresTransformer hologresTransformer;

    @Before
    public void setUp() {
        hologresTransformer = new HologresTransformer();
    }

    @Test
    public void testLogicalPartitionTableConversion() throws IOException {
        // Read the test resource file
        String resourceContent = IOUtils.resourceToString("/hologres/logic_partition.txt", StandardCharsets.UTF_8);

        // Parse the DialectNode from the resource content
        DialectNode dialectNode = new DialectNode(resourceContent);

        // Reverse the dialect node to get the FML node
        Node fmlNode = hologresTransformer.reverse(dialectNode, ReverseContext.builder().build());

        // Transform the FML node to Table object
        Table table = hologresTransformer.transformTable(fmlNode, TransformContext.builder().build());

        // Verify the table is not null
        assertNotNull("Table object should not be null", table);

        // Verify table name
        assertEquals("dwd_sales_trans_log_di_rt", table.getName());

        // Verify schema name
        assertEquals("public", table.getSchema());

        // Verify columns
        assertEquals(2, table.getColumns().size());

        // Verify first column
        assertEquals("log_seq", table.getColumns().get(0).getName());
        assertEquals("BIGINT", table.getColumns().get(0).getDataType());
        assertEquals(false, table.getColumns().get(0).isNullable());

        // Verify second column
        assertEquals("ds", table.getColumns().get(1).getName());
        assertEquals("TEXT", table.getColumns().get(1).getDataType());
        assertEquals(false, table.getColumns().get(1).isNullable());

        // Verify primary key constraint
        assertEquals(1, table.getConstraints().size());
        assertEquals("primary", table.getConstraints().get(0).getType().getCode());
        assertEquals(2, table.getConstraints().get(0).getColumns().size());
        assertEquals("log_seq", table.getConstraints().get(0).getColumns().get(0));
        assertEquals("ds", table.getConstraints().get(0).getColumns().get(1));

        // Verify properties
        assertEquals(4, table.getProperties().size());
        // Check orientation property
        assertEquals("orientation", table.getProperties().get(0).getKey());

        // Check storage_format property
        assertEquals("storage_format", table.getProperties().get(1).getKey());
        assertEquals("orc", table.getProperties().get(1).getValue());
        // Check binlog_level property
        assertEquals("binlog_level", table.getProperties().get(2).getKey());
        assertEquals("replica", table.getProperties().get(2).getValue());
        assertEquals("true", table.getProperties().get(3).getValue());

    }
}