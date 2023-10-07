/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
import com.aliyun.fastmodel.transform.hologres.client.property.SegmentKey;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * cluster test
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class SegementKeyTest extends BaseGeneratorTest {

    @Test
    public void testClusterKey() {
        List<BaseClientProperty> properties = new ArrayList<>();
        SegmentKey e = new SegmentKey();
        e.setValue(Lists.newArrayList("c1", "c2"));
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
            + "CALL SET_TABLE_PROPERTY('abc', 'segment_key', '\"c1,c2\"');\n"
            + "COMMIT;");
        generate = codeGenerator.generate(DdlGeneratorModelRequest.builder().after(table).config(TableConfig.builder()
                .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2))
                .build())
            .build());
        dialectNode = generate.getDialectNodes().get(0);
        node = dialectNode.getNode();
        assertEquals(node, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS abc;\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'segment_key', '\"c1\",\"c2\"');\n"
            + "COMMIT;");
    }
}
