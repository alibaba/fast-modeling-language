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

package com.aliyun.fastmodel.transform.clickhouse.client.generator;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import com.aliyun.fastmodel.transform.clickhouse.ClickHouseTransformer;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseLanguageParser;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * ClickHouse 建表 DDL 生成的测试,链路与 DefaultCodeGenerator 的默认建表链路一致
 *
 * @author panguanjing
 */
public class ClickHouseGeneratorTest {

    private final CodeGenerator codeGenerator = new DefaultCodeGenerator();

    private final ClickHouseTransformer clickHouseTransformer = new ClickHouseTransformer();

    private final ClickHouseLanguageParser clickHouseLanguageParser = new ClickHouseLanguageParser();

    @Test
    public void testGenerateCreateTable() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).primaryKey(true).build());
        columns.add(Column.builder().name("name").dataType("VARCHAR").length(32).nullable(true).comment("名称").build());
        columns.add(Column.builder().name("create_time").dataType("DATETIME").nullable(false).build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_PARTITION_BY.getValue(), "toYYYYMM(create_time)"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "(id, create_time)"));
        Table after = Table.builder()
            .ifNotExist(false)
            .database("db")
            .name("di_table")
            .comment("建表示例")
            .columns(columns)
            .properties(properties)
            .build();
        DdlGeneratorResult result = generate(after);
        assertEquals("CREATE TABLE db.di_table\n"
            + "(\n"
            + "   id          Int64,\n"
            + "   name        Nullable(String) COMMENT '名称',\n"
            + "   create_time DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMM(create_time)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id, create_time)\n"
            + "COMMENT '建表示例';", join(result));
    }

    @Test
    public void testGenerateWithConstraintPrimaryKey() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("sub_id").dataType("INT").nullable(false).build());
        columns.add(Column.builder().name("value").dataType("DOUBLE").nullable(true).build());
        List<Constraint> constraints = Lists.newArrayList();
        constraints.add(Constraint.builder()
            .columns(Lists.newArrayList("id", "sub_id"))
            .type(ExtensionClientConstraintType.PRIMARY_KEY)
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "ReplacingMergeTree"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "(id, sub_id)"));
        Table after = Table.builder()
            .ifNotExist(false)
            .name("di_table")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
        DdlGeneratorResult result = generate(after);
        assertEquals("CREATE TABLE di_table\n"
            + "(\n"
            + "   id     Int64,\n"
            + "   sub_id Int32,\n"
            + "   value  Nullable(Float64)\n"
            + ")\n"
            + "ENGINE = ReplacingMergeTree\n"
            + "PRIMARY KEY (id,sub_id)\n"
            + "ORDER BY (id, sub_id);", join(result));
    }

    @Test
    public void testGenerateFromReverseDdl() {
        String ddl = "CREATE TABLE db.demo\n"
            + "(\n"
            + "    id Int64,\n"
            + "    name Nullable(String),\n"
            + "    create_time DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id)";
        BaseStatement reverse = clickHouseTransformer.reverse(new DialectNode(ddl));
        Table table = clickHouseTransformer.transformTable(reverse, TransformContext.builder().build());
        table.setIfNotExist(false);
        DdlGeneratorResult result = generate(table);
        assertEquals("CREATE TABLE db.demo\n"
            + "(\n"
            + "   id          Int64,\n"
            + "   name        Nullable(String),\n"
            + "   create_time DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id);", join(result));
        assertNotNull(clickHouseLanguageParser.parseNode(join(result)));
    }

    @Test
    public void testGenerateWithPartitionColumnsAndCluster() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).primaryKey(true).build());
        columns.add(Column.builder().name("dt").dataType("String").nullable(false)
            .partitionKey(true).partitionKeyIndex(0).build());
        columns.add(Column.builder().name("amount").dataType("DECIMAL").precision(18).scale(4).nullable(true).build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_CLUSTER.getValue(), "{cluster}"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "(id, dt)"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_SETTINGS.getValue(),
            "index_granularity = 8192"));
        Table after = Table.builder()
            .ifNotExist(false)
            .database("db")
            .name("di_partition")
            .columns(columns)
            .properties(properties)
            .build();
        DdlGeneratorResult result = generate(after);
        assertEquals("CREATE TABLE db.di_partition ON CLUSTER {cluster}\n"
            + "(\n"
            + "   id     Int64,\n"
            + "   dt     String,\n"
            + "   amount Nullable(Decimal(18,4))\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY (dt)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192;", join(result));
    }

    @Test
    public void testGeneratePrimaryKeyWithMultipleColumns() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("a").dataType("BIGINT").nullable(false).primaryKey(true).build());
        columns.add(Column.builder().name("b").dataType("INT").nullable(false).primaryKey(true).build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "(a, b)"));
        Table after = Table.builder()
            .ifNotExist(false)
            .name("di_pk")
            .columns(columns)
            .properties(properties)
            .build();
        assertEquals("CREATE TABLE di_pk\n"
            + "(\n"
            + "   a Int64,\n"
            + "   b Int32\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PRIMARY KEY (a,b)\n"
            + "ORDER BY (a, b);", join(generate(after)));
    }

    private DdlGeneratorResult generate(Table table) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.getByNameAndVersion(DialectName.CLICKHOUSE.getValue(), IVersion.DEFAULT_VERSION))
                .filterProperties(false)
                .build())
            .build();
        return codeGenerator.generate(request);
    }

    private String join(DdlGeneratorResult result) {
        return result.getDialectNodes().stream().map(DialectNode::getNode).collect(Collectors.joining("\n"));
    }
}
