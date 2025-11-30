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

package com.aliyun.fastmodel.transform.hologres.client.generator;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.property.HologresPropertyKey;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogicalPartitionedByTest {

    private CodeGenerator codeGenerator;

    @Before
    public void setUp() {
        codeGenerator = new DefaultCodeGenerator();
    }

    @Test
    public void testGeneratorLogicalPartitionTable() {
        // Create a table with logical partitioning
        Table table = getLogicalPartitionTable();

        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();

        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();

        // Verify the generated SQL matches expected format
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS public.dwd_sales_trans_log_di_rt (\n"
            + "   log_seq BIGINT NOT NULL,\n"
            + "   ds      TEXT NOT NULL,\n"
            + "   PRIMARY KEY(log_seq,ds)\n"
            + ")LOGICAL PARTITION BY LIST(ds);\n"
            + "CALL SET_TABLE_PROPERTY('public.dwd_sales_trans_log_di_rt', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('public.dwd_sales_trans_log_di_rt', 'storage_mode', 'orc');\n"
            + "CALL SET_TABLE_PROPERTY('public.dwd_sales_trans_log_di_rt', 'binlog.ttl', '10');\n"
            + "COMMIT;", dialectNodes.get(0).getNode());
    }

    private Table getLogicalPartitionTable() {
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("log_seq")
                .dataType("bigint")
                .nullable(false)
                .primaryKey(true)
                .build(),
            Column.builder()
                .name("ds")
                .dataType("text")
                .nullable(false)
                .primaryKey(true)
                .partitionKey(true)
                .partitionKeyIndex(1)
                .build()
        );

        // Add properties
        List<BaseClientProperty> properties = Lists.newArrayList();

        StringProperty orientationProperty = new StringProperty();
        orientationProperty.setKey(HologresPropertyKey.ORIENTATION.getValue());
        orientationProperty.setValue("column");
        properties.add(orientationProperty);

        StringProperty storageFormatProperty = new StringProperty();
        storageFormatProperty.setKey(HologresPropertyKey.STORAGE_MODE.getValue());
        storageFormatProperty.setValue("orc");
        properties.add(storageFormatProperty);

        StringProperty binlogLevelProperty = new StringProperty();
        binlogLevelProperty.setKey(HologresPropertyKey.BINLOG_TTL.getValue());
        binlogLevelProperty.setValue("10");
        properties.add(binlogLevelProperty);

        StringProperty logic = new StringProperty();
        logic.setKey(HologresPropertyKey.LOGIC_PARTITIONED_BY.getValue());
        logic.setValue("true");
        properties.add(logic);

        return Table.builder()
            .schema("public")
            .name("dwd_sales_trans_log_di_rt")
            .columns(columns)
            .properties(properties)
            .build();
    }
}