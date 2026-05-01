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

package com.aliyun.fastmodel.transform.hologres.client.generator.impl;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ClusterKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ColumnOrder;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * cluster test
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class ClusterKeyTest extends BaseGeneratorTest {

    @Test
    public void testClusterKey() {
        List<BaseClientProperty> properties = new ArrayList<>();
        ClusterKey e = new ClusterKey();
        List<ColumnOrder> orderList = ColumnOrder.of("c1, c2");
        e.setValue(orderList);
        properties.add(
            e
        );
        Table table = Table.builder()
            .name("abc")
            .properties(properties)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(DdlGeneratorModelRequest.builder().after(table).config(TableConfig.builder()
                .dialectMeta(DialectMeta.getHologres())
                .build())
            .build());
        int size = generate.getDialectNodes().size();
        assertEquals(1, size);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        String node = dialectNode.getNode();
        assertEquals(node, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS abc;\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'clustering_key', '\"c1,c2\"');\n"
            + "COMMIT;");
    }

    @Test
    public void testClusterKeyAsc() {
        List<BaseClientProperty> properties = new ArrayList<>();
        ClusterKey e = new ClusterKey();
        List<ColumnOrder> orderList = ColumnOrder.of("c1:asc, c2:desc");
        e.setValue(orderList);
        properties.add(
            e
        );
        Table table = Table.builder()
            .name("abc")
            .properties(properties)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(DdlGeneratorModelRequest.builder().after(table).config(TableConfig.builder()
                .dialectMeta(DialectMeta.getHologres())
                .build())
            .build());
        int size = generate.getDialectNodes().size();
        assertEquals(1, size);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        String node = dialectNode.getNode();
        assertEquals(node, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS abc;\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'clustering_key', '\"c1:ASC,c2:DESC\"');\n"
            + "COMMIT;");
    }

    @Test
    public void testClusterKeyAsc2() {
        List<BaseClientProperty> properties = new ArrayList<>();
        ClusterKey e = new ClusterKey();
        List<ColumnOrder> orderList = ColumnOrder.of("c1:asc, c2");
        e.setValue(orderList);
        properties.add(
            e
        );
        Table table = Table.builder()
            .name("abc")
            .properties(properties)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(DdlGeneratorModelRequest.builder().after(table).config(TableConfig.builder()
                .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2))
                .build())
            .build());
        int size = generate.getDialectNodes().size();
        assertEquals(1, size);
        DialectNode dialectNode = generate.getDialectNodes().get(0);
        String node = dialectNode.getNode();
        assertEquals(node, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS abc;\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'clustering_key', '\"c1\":ASC,\"c2\"');\n"
            + "COMMIT;");
    }
}
