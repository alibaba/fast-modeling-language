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

package com.aliyun.fastmodel.transform.adbmysql.client;

import java.util.List;

import com.aliyun.fastmodel.transform.adbmysql.client.property.AdbMysqlBlockSize;
import com.aliyun.fastmodel.transform.adbmysql.client.property.AdbMysqlHotPartitionCount;
import com.aliyun.fastmodel.transform.adbmysql.client.property.AdbMysqlStoragePolicy;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TimeExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.TimeExpressionClientPartition;
import com.google.common.collect.Lists;
import org.junit.Test;

/**
 * <a href="https://help.aliyun.com/zh/analyticdb-for-mysql/use-cases/schema-design?spm=a2c4g.11186623.0.0.47563f47boqajz#section-4s5-di5-bjt">...</a>
 * adb mysql generator
 *
 * @author panguanjing
 * @date 2024/3/19
 */
public class AdbMysqlGeneratorClientTest extends BaseAdbMysqlTest {

    CodeGenerator codeGenerator = new DefaultCodeGenerator();

    @Test
    public void testGeneratorPartition() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        columns.add(c);
        List<Constraint> constraints = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        AdbMysqlHotPartitionCount e = new AdbMysqlHotPartitionCount();
        e.setValue(10L);
        properties.add(e);

        AdbMysqlBlockSize adbMysqlBlockSize = new AdbMysqlBlockSize();
        adbMysqlBlockSize.setValueString("10");
        properties.add(adbMysqlBlockSize);

        AdbMysqlStoragePolicy adbMysqlStoragePolicy = new AdbMysqlStoragePolicy();
        adbMysqlStoragePolicy.setValue("MIXED");
        properties.add(adbMysqlStoragePolicy);

        TimeExpressionPartitionProperty timeExpressionPartitionProperty = new TimeExpressionPartitionProperty();
        TimeExpressionClientPartition partitionValue = new TimeExpressionClientPartition();
        partitionValue.setColumn("c1");
        partitionValue.setFuncName("date_format");
        partitionValue.setTimeUnit("%Y%M%d");
        timeExpressionPartitionProperty.setValue(partitionValue);

        properties.add(timeExpressionPartitionProperty);

        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment'\n"
            + ")\n"
            + "PARTITION BY VALUE(date_format(c1, '%Y%M%d'))\n"
            + "HOT_PARTITION_COUNT=10\n"
            + "BLOCK_SIZE=10\n"
            + "STORAGE_POLICY='MIXED'", table);
    }

    @Test
    public void testGeneratorPartitionWithRaw() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        columns.add(c);
        List<Constraint> constraints = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();

        AdbMysqlBlockSize adbMysqlBlockSize = new AdbMysqlBlockSize();
        adbMysqlBlockSize.setValueString("10");
        properties.add(adbMysqlBlockSize);

        AdbMysqlStoragePolicy adbMysqlStoragePolicy = new AdbMysqlStoragePolicy();
        adbMysqlStoragePolicy.setValue("MIXED");
        properties.add(adbMysqlStoragePolicy);

        AdbMysqlHotPartitionCount e = new AdbMysqlHotPartitionCount();
        e.setValue(10L);
        properties.add(e);

        TablePartitionRaw tablePartitionRaw = new TablePartitionRaw();
        tablePartitionRaw.setValueString("PARTITION BY VALUE(date_format(c1, '%Y%M%d'))");

        properties.add(tablePartitionRaw);

        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment'\n"
            + ")\n"
            + "PARTITION BY VALUE(date_format(c1, '%Y%M%d'))\n"
            + "BLOCK_SIZE=10\n"
            + "STORAGE_POLICY='MIXED'\n"
            + "HOT_PARTITION_COUNT=10", table);
    }

    @Test
    public void testGeneratorProperty() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        columns.add(c);
        List<Constraint> constraints = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        AdbMysqlHotPartitionCount e = new AdbMysqlHotPartitionCount();
        e.setValue(10L);
        properties.add(e);

        AdbMysqlBlockSize adbMysqlBlockSize = new AdbMysqlBlockSize();
        adbMysqlBlockSize.setValueString("10");
        properties.add(adbMysqlBlockSize);

        AdbMysqlStoragePolicy adbMysqlStoragePolicy = new AdbMysqlStoragePolicy();
        adbMysqlStoragePolicy.setValue("MIXED");
        properties.add(adbMysqlStoragePolicy);

        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment'\n"
            + ")\n"
            + "HOT_PARTITION_COUNT=10\n"
            + "BLOCK_SIZE=10\n"
            + "STORAGE_POLICY='MIXED'", table);
    }

    @Test
    public void testGeneratorDistribute() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        Column c2 = Column.builder()
            .name("c2")
            .nullable(true)
            .dataType("varchar")
            .length(10)
            .comment("comment")
            .build();
        columns.add(c);
        columns.add(c2);
        List<Constraint> constraints = Lists.newArrayList();
        DistributeClientConstraint distributeClientConstraint = new DistributeClientConstraint();
        List<String> distributeColumns = Lists.newArrayList("c2");
        distributeClientConstraint.setColumns(distributeColumns);
        distributeClientConstraint.setBucket(10);
        constraints.add(distributeClientConstraint);

        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment',\n"
            + "   c2 VARCHAR(10) NULL COMMENT 'comment'\n"
            + ")\n"
            + "DISTRIBUTE BY HASH(c2)", table);
    }

    @Test
    public void testGeneratorLifecycle() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        columns.add(c);
        List<Constraint> constraints = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        TablePartitionRaw tablePartitionRaw = new TablePartitionRaw();
        tablePartitionRaw.setValueString("PARTITION BY VALUE(date_format(c1, '%Y%M%d'))");
        properties.add(tablePartitionRaw);
        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .lifecycleSeconds(1000000L)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment'\n"
            + ")\n"
            + "PARTITION BY VALUE(date_format(c1, '%Y%M%d')) LIFECYCLE 11", table);
    }

    @Test
    public void testGeneratorDatabase() {
        List<Column> columns = Lists.newArrayList();
        Column c = Column.builder()
            .name("c1")
            .nullable(true)
            .dataType("bigint")
            .comment("comment")
            .build();
        columns.add(c);
        List<Constraint> constraints = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        TablePartitionRaw tablePartitionRaw = new TablePartitionRaw();
        tablePartitionRaw.setValueString("PARTITION BY VALUE(date_format(c1, '%Y%M%d'))");
        properties.add(tablePartitionRaw);
        Table table = Table.builder().name("t1")
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .lifecycleSeconds(1000000L)
            .database("db1")
            .build();
        assertText("CREATE TABLE IF NOT EXISTS db1.t1\n"
            + "(\n"
            + "   c1 BIGINT NULL COMMENT 'comment'\n"
            + ")\n"
            + "PARTITION BY VALUE(date_format(c1, '%Y%M%d')) LIFECYCLE 11", table);
    }

    @Test
    public void testGeneratorPartitionByHash() {

    }

    private void assertText(String expect, Table table) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_ADB_MYSQL)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        assertEquals(expect, generate.getDialectNodes().get(0).getNode());
    }
}
