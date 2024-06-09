/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresPropertyConverter;
import com.aliyun.fastmodel.transform.hologres.client.property.BinLogTTL;
import com.aliyun.fastmodel.transform.hologres.client.property.ClusterKey;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel.BinLogLevel;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test set property
 *
 * @author panguanjing
 * @date 2022/7/12
 */
public class HologresCodeGeneratorSetPropertyTest {
    @Test
    public void testSetProperty() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        BaseClientProperty baseClientProperty = HologresPropertyConverter.getInstance().create(EnableBinLogLevel.ENABLE_BINLOG,
            BinLogLevel.REPLICA.getValue());
        List<BaseClientProperty> beforeProperties = ImmutableList.of(baseClientProperty);
        BaseClientProperty baseClientProperty1 = HologresPropertyConverter.getInstance().create(EnableBinLogLevel.ENABLE_BINLOG,
            BinLogLevel.NONE.getValue());
        BaseClientProperty bingLogTTL = HologresPropertyConverter.getInstance().create(BinLogTTL.BINLOG_TTL, "10000");
        List<BaseClientProperty> afterProperty = ImmutableList.of(baseClientProperty1, bingLogTTL);
        List<Column> columns = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").properties(beforeProperties).build())
            .after(Table.builder().name("a").properties(afterProperty).columns(columns).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(result, "BEGIN;\n"
            + "ALTER TABLE IF EXISTS a ADD COLUMN c1 TEXT;\n"
            + "COMMIT;\n"
            + "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'binlog.level', 'none');\n"
            + "CALL SET_TABLE_PROPERTY('a', 'binlog.ttl', '10000');\n"
            + "COMMIT;");
    }

    @Test
    public void testSetPropertyEncodingColumnsV1() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<BaseClientProperty> properties = Lists.newArrayList();
        ClusterKey clusterKey = new ClusterKey();
        clusterKey.setValue(Lists.newArrayList("int", "double"));
        properties.add(clusterKey);
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("int").dataType("int4").build());
        columns.add(Column.builder().name("double").dataType("int8").build());
        Table table = Table.builder()
            .name("t1")
            .columns(columns)
            .properties(properties)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).caseSensitive(true).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String node = generate.getDialectNodes().get(0).getNode();
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t1 (\n"
            + "   \"int\"    INTEGER NOT NULL,\n"
            + "   \"double\" BIGINT NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('t1', 'clustering_key', '\"int,double\"');\n"
            + "COMMIT;", node);
    }

    @Test
    public void testSetPropertyEncodingColumnsV2() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<BaseClientProperty> properties = Lists.newArrayList();
        ClusterKey clusterKey = new ClusterKey();
        clusterKey.setValue(Lists.newArrayList("int", "double"));
        properties.add(clusterKey);
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("int").dataType("int4").build());
        columns.add(Column.builder().name("double").dataType("int8").build());
        Table table = Table.builder()
            .name("t1")
            .columns(columns)
            .properties(properties)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2))
                .caseSensitive(true).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String node = generate.getDialectNodes().get(0).getNode();
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t1 (\n"
            + "   \"int\"    INTEGER NOT NULL,\n"
            + "   \"double\" BIGINT NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('t1', 'clustering_key', '\"int\",\"double\"');\n"
            + "COMMIT;", node);
    }

    @Test
    public void testSetPropertyWithSchema() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<BaseClientProperty> properties = Lists.newArrayList();
        EnableBinLogLevel binLogLevel = new EnableBinLogLevel();
        properties.add(binLogLevel);
        Table after = Table.builder()
            .name("t1")
            .database("jiawatest")
            .schema("autodb")
            .properties(properties)
            .build();

        Table before = Table.builder()
            .name("t1")
            .database("jiawatest")
            .schema("autodb")
            .build();
        TableConfig config = TableConfig.builder().dialectMeta(DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2))
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(before)
            .after(after)
            .config(config)
            .build();

        DdlGeneratorResult generate = codeGenerator.generate(request);
        String node = generate.getDialectNodes().get(0).getNode();
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('autodb.t1', 'binlog.level', 'none');\n"
            + "COMMIT;", node);

    }
}
