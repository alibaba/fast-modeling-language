/*
 * Copyright 2021-2023 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 子梁
 * @date 2023/8/22
 */
public class CodeGeneratorTest {

    // All SQL, storage locations and account placeholders below are synthetic fixtures.

    CodeGenerator ddlGenerator = new DefaultCodeGenerator();

    @Test
    public void testMysqlCreateTable() {
        // 测试MySQL创建表的处理
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("CREATE TABLE test_table (\n" +
            "  id INT NOT NULL AUTO_INCREMENT COMMENT '主键',\n" +
            "  name VARCHAR(100) NOT NULL COMMENT '名称',\n" +
            "  age INT COMMENT '年龄',\n" +
            "  email VARCHAR(255) COMMENT '邮箱',\n" +
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
            "  PRIMARY KEY (id),\n" +
            "  UNIQUE KEY uk_name (name),\n" +
            "  KEY idx_email (email)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试表';");
        request.setDialectMeta(DialectMeta.DEFAULT_MYSQL);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertEquals("test_table", table.getName());
        Assert.assertEquals("测试表", table.getComment());
        Assert.assertEquals(5, table.getColumns().size());

        // 验证列信息
        Column idColumn = table.getColumns().get(0);
        Assert.assertEquals("id", idColumn.getName());
        Assert.assertEquals("INT", idColumn.getDataType());
        Assert.assertEquals(false, idColumn.isNullable());
        Assert.assertEquals("主键", idColumn.getComment());

        Column nameColumn = table.getColumns().get(1);
        Assert.assertEquals("name", nameColumn.getName());
        Assert.assertEquals("VARCHAR", nameColumn.getDataType());
        Assert.assertEquals(100, nameColumn.getLength().intValue());
        Assert.assertEquals(false, nameColumn.isNullable());
        Assert.assertEquals("名称", nameColumn.getComment());

        Column ageColumn = table.getColumns().get(2);
        Assert.assertEquals("age", ageColumn.getName());
        Assert.assertEquals("INT", ageColumn.getDataType());
        Assert.assertEquals(true, ageColumn.isNullable());
        Assert.assertEquals("年龄", ageColumn.getComment());

        // 测试反向生成
        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MYSQL).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertNotNull(generatorResult.getDialectNodes());
        Assert.assertTrue(generatorResult.getDialectNodes().size() > 0);
    }

    @Test
    public void testMysqlReverseTable() {
        // 创建一个Table对象用于测试reverseTable方法
        Table table = Table.builder()
            .name("test_reverse_table")
            .comment("测试反向表")
            .columns(Arrays.asList(
                Column.builder()
                    .name("id")
                    .dataType("INT")
                    .nullable(false)
                    .comment("主键")
                    .build(),
                Column.builder()
                    .name("name")
                    .dataType("VARCHAR")
                    .length(100)
                    .nullable(false)
                    .comment("名称")
                    .build(),
                Column.builder()
                    .name("age")
                    .dataType("INT")
                    .nullable(true)
                    .comment("年龄")
                    .build()
            ))
            .build();

        // 使用MySQL转换器的reverseTable方法
        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MYSQL).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);

        Assert.assertNotNull(generatorResult.getDialectNodes());
        Assert.assertTrue(generatorResult.getDialectNodes().size() > 0);

        String generatedDdl = generatorResult.getDialectNodes().get(0).getNode();
        Assert.assertNotNull(generatedDdl);
        Assert.assertTrue(generatedDdl.contains("test_reverse_table"));
        Assert.assertTrue(generatedDdl.contains("id"));
        Assert.assertTrue(generatedDdl.contains("name"));
        Assert.assertTrue(generatedDdl.contains("age"));
    }

    @Test
    public void reverseCreateComplexDataTypeTableDdl() {
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create table example_project.public.test_sku(\n" +
            "                    code STRING not null comment '编码',\n" +
            "                    price DECIMAL(20,4) NOT null comment '价格',\n" +
            "                    produce_time DATE NOT NULL comment '生产日期',\n" +
            "                    band VARCHAR(5) comment '品牌',\n" +
            "                    c_list ARRAY<INT> comment 'c列表',\n" +
            "                    c_map MAP<STRING, DATETIME> comment 'cMap',\n" +
            "                    c_struct STRUCT<code:STRING> comment 'cStruct',\n" +
            "                    primary key (code)\n" +
            "                ) comment '商品表'\n" +
            "                partitioned by (ds STRING comment '分区', region STRING comment '区域')\n" +
            "                clustered by (code) sorted by (code) into 512 buckets\n" +
            "                lifecycle 333;");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertEquals("test_sku", table.getName());
        Assert.assertEquals("商品表", table.getComment());
        Assert.assertEquals(9, table.getColumns().size());
        Assert.assertEquals("example_project", table.getDatabase());
        Assert.assertEquals("public", table.getSchema());
        Assert.assertEquals(333 * 24 * 60 * 60L, table.getLifecycleSeconds().longValue());
        // primary key
        Assert.assertEquals(2, table.getConstraints().size());
        Constraint constraint = table.getConstraints().get(0);
        Assert.assertEquals(ExtensionClientConstraintType.CLUSTER_BY, constraint.getType());

        constraint = table.getConstraints().get(1);
        Assert.assertEquals(OutlineConstraintType.PRIMARY_KEY, constraint.getType());
        Assert.assertEquals(1, constraint.getColumns().size());
        Assert.assertEquals("code", constraint.getColumns().get(0));

        Column column1 = table.getColumns().get(0);
        Assert.assertEquals("code", column1.getName());
        Assert.assertEquals("编码", column1.getComment());
        Assert.assertEquals("STRING", column1.getDataType());
        Assert.assertEquals(false, column1.isNullable());

        Column column2 = table.getColumns().get(1);
        Assert.assertEquals("price", column2.getName());
        Assert.assertEquals("价格", column2.getComment());
        Assert.assertEquals("DECIMAL", column2.getDataType());
        Assert.assertEquals(20, column2.getPrecision().intValue());
        Assert.assertEquals(4, column2.getScale().intValue());
        Assert.assertEquals(false, column2.isNullable());

        Column column3 = table.getColumns().get(2);
        Assert.assertEquals("DATE", column3.getDataType());

        Column column4 = table.getColumns().get(3);
        Assert.assertEquals("VARCHAR", column4.getDataType());
        Assert.assertEquals(5, column4.getLength().intValue());
        Assert.assertEquals(true, column4.isNullable());

        Column column5 = table.getColumns().get(4);
        Assert.assertEquals("ARRAY<INT>", column5.getDataType());
        Assert.assertEquals(true, column5.isNullable());

        Column column6 = table.getColumns().get(5);
        Assert.assertEquals("MAP<STRING, DATETIME>", column6.getDataType());
        Assert.assertEquals(true, column6.isNullable());

        Column column7 = table.getColumns().get(6);
        Assert.assertEquals("STRUCT<code:STRING>", column7.getDataType());
        Assert.assertEquals(true, column7.isNullable());

        Column column8 = table.getColumns().get(7);
        Assert.assertEquals(true, column8.isPartitionKey());

        Column column9 = table.getColumns().get(8);
        Assert.assertEquals(true, column9.isPartitionKey());
    }

    @Test
    public void reverseCreateExternalTableDdl1() {
        // 内置文本数据解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table if not exists mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.CsvStorageHandler' \n" +
            "with serdeproperties (\n" +
            " 'odps.properties.rolearn'='acs:ram::xxxxxx:role/aliyunodpsdefaultrole',\n" +
            " 'odps.text.option.gzip.input.enabled'='true',\n" +
            " 'odps.text.option.gzip.output.enabled'='true' \n" +
            ") \n" +
            "location 'oss://example-bucket/synthetic-format-1/';");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertTrue(table.isExternal());
        Assert.assertEquals("oss://example-bucket/synthetic-format-1/", table.getProperties().get(1).getValue());
        Assert.assertEquals("com.aliyun.odps.CsvStorageHandler", table.getProperties().get(2).getValue());
        Assert.assertEquals("serde_props.odps.properties.rolearn", table.getProperties().get(3).getKey());
        Assert.assertEquals("acs:ram::xxxxxx:role/aliyunodpsdefaultrole", table.getProperties().get(3).getValue());
        Assert.assertEquals("serde_props.odps.text.option.gzip.input.enabled", table.getProperties().get(4).getKey());
        Assert.assertEquals("true", table.getProperties().get(4).getValue());
        Assert.assertEquals("serde_props.odps.text.option.gzip.output.enabled", table.getProperties().get(5).getKey());
        Assert.assertEquals("true", table.getProperties().get(5).getValue());
    }

    @Test
    public void reverseCreateExternalTableDdl2() {
        // 内置开源数据解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table if not exists mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "stored as textfile\n" +
            "location 'oss://example-bucket/synthetic-format-1/';");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertTrue(table.isExternal());
        Assert.assertEquals("oss://example-bucket/synthetic-format-1/", table.getProperties().get(1).getValue());
        Assert.assertEquals("textfile", table.getProperties().get(2).getValue());
        Assert.assertEquals("org.apache.hive.hcatalog.data.JsonSerDe", table.getProperties().get(3).getValue());
    }

    @Test
    public void reverseCreateExternalTableDdl3() {
        // 自定义解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table if not exists example_external_events\n" +
            "(\n" +
            "vehicleId int,\n" +
            "recordId int,\n" +
            "subjectId int,\n" +
            "calls int,\n" +
            "locationLatitute double,\n" +
            "locationLongtitue double,\n" +
            "recordTime string,\n" +
            "direction string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.udf.example.text.TextStorageHandler' \n" +
            "  with serdeproperties (\n" +
            "'delimiter'='|',  \n" +
            "'odps.properties.rolearn'='acs:ram::xxxxxxxxxxxxx:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "location 'oss://example-bucket/synthetic-format-2/'\n" +
            "using 'javatest-1.0-SNAPSHOT.jar'; ");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertTrue(table.isExternal());
        Assert.assertEquals("oss://example-bucket/synthetic-format-2/", table.getProperties().get(1).getValue());
        Assert.assertEquals("com.aliyun.odps.udf.example.text.TextStorageHandler", table.getProperties().get(2).getValue());
        Assert.assertEquals("|", table.getProperties().get(3).getValue());
        Assert.assertEquals("acs:ram::xxxxxxxxxxxxx:role/aliyunodpsdefaultrole", table.getProperties().get(4).getValue());
        Assert.assertEquals("javatest-1.0-SNAPSHOT.jar", table.getProperties().get(5).getValue());
    }

    @Test
    public void reverseCreateExternalTableDdl4() {
        // 自定义解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("CREATE EXTERNAL TABLE IF NOT EXISTS example_project.example_external_table" +
            "(" +
            "c1 STRING COMMENT '字段描述1'" +
            ",c2 STRING COMMENT '字段描述2'" +
            ",c3 STRING COMMENT '字段描述3'" +
            ") " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' " +
            "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.TextOutputFormat' " +
            "LOCATION 'oss://example-bucket/synthetic-format-3/' " +
            "TBLPROPERTIES ('comment'='表描述');");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();
        Assert.assertTrue(table.isExternal());
        Assert.assertEquals("oss://example-bucket/synthetic-format-3/", table.getProperties().get(2).getValue());
        Assert.assertEquals("org.apache.hadoop.mapred.TextInputFormat", table.getProperties().get(3).getValue());
        Assert.assertEquals("org.apache.hadoop.mapred.TextOutputFormat", table.getProperties().get(4).getValue());
        Assert.assertEquals("org.apache.hive.hcatalog.data.JsonSerDe", table.getProperties().get(5).getValue());
    }

    @Test
    public void testGenerateCreateExternalTableDdl1() {
        // 内置文本数据解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.CsvStorageHandler' \n" +
            "with serdeproperties (\n" +
            " 'odps.properties.rolearn'='acs:ram::xxxxxx:role/aliyunodpsdefaultrole',\n" +
            " 'odps.text.option.gzip.input.enabled'='true',\n" +
            " 'odps.text.option.gzip.output.enabled'='true' \n" +
            ") \n" +
            "location 'oss://example-bucket/synthetic-format-1/';");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);
        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();

        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertEquals("CREATE EXTERNAL TABLE mc_oss_csv_external3\n" +
            "(\n" +
            "   vehicleid        INT,\n" +
            "   locationlatitute DOUBLE,\n" +
            "   recordtime       STRING\n" +
            ")\n" +
            "STORED BY 'com.aliyun.odps.CsvStorageHandler'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'odps.properties.rolearn' = 'acs:ram::xxxxxx:role/aliyunodpsdefaultrole',\n" +
            "   'odps.text.option.gzip.input.enabled' = 'true',\n" +
            "   'odps.text.option.gzip.output.enabled' = 'true'\n" +
            ")\n" +
            "LOCATION 'oss://example-bucket/synthetic-format-1/'\n" +
            ";", generatorResult.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGenerateCreateExternalTableDdl2() {
        // 内置开源数据解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "stored as textfile\n" +
            "location 'oss://example-bucket/synthetic-format-1/';");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();

        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertEquals("CREATE EXTERNAL TABLE mc_oss_csv_external3\n" +
            "(\n" +
            "   vehicleid        INT,\n" +
            "   locationlatitute DOUBLE,\n" +
            "   recordtime       STRING\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://example-bucket/synthetic-format-1/'\n" +
            ";", generatorResult.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGenerateCreateExternalTableDdl3() {
        // 自定义解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("create external table example_external_events\n" +
            "(\n" +
            "vehicleId int,\n" +
            "recordId int,\n" +
            "subjectId int,\n" +
            "calls int,\n" +
            "locationLatitute double,\n" +
            "locationLongtitue double,\n" +
            "recordTime string,\n" +
            "direction string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.udf.example.text.TextStorageHandler' \n" +
            "  with serdeproperties (\n" +
            "'delimiter'='|',  \n" +
            "'odps.properties.rolearn'='acs:ram::xxxxxxxxxxxxx:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "location 'oss://example-bucket/synthetic-format-2/'\n" +
            "using 'javatest-1.0-SNAPSHOT.jar'; ");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);

        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();

        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertEquals("CREATE EXTERNAL TABLE example_external_events\n" +
            "(\n" +
            "   vehicleid         INT,\n" +
            "   recordid          INT,\n" +
            "   subjectid         INT,\n" +
            "   calls             INT,\n" +
            "   locationlatitute  DOUBLE,\n" +
            "   locationlongtitue DOUBLE,\n" +
            "   recordtime        STRING,\n" +
            "   direction         STRING\n" +
            ")\n" +
            "STORED BY 'com.aliyun.odps.udf.example.text.TextStorageHandler'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'delimiter' = '|',\n" +
            "   'odps.properties.rolearn' = 'acs:ram::xxxxxxxxxxxxx:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "LOCATION 'oss://example-bucket/synthetic-format-2/'\n" +
            "USING 'javatest-1.0-SNAPSHOT.jar'\n" +
            ";", generatorResult.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGenerateCreateExternalTableDdl4() {
        // 内置文本数据解析器
        DdlReverseSqlRequest request = new DdlReverseSqlRequest();
        request.setCode("CREATE EXTERNAL TABLE IF NOT EXISTS example_project.example_external_table" +
            "(" +
            "c1 STRING COMMENT '字段描述1'" +
            ",c2 STRING COMMENT '字段描述2'" +
            ",c3 STRING COMMENT '字段描述3'" +
            ") " +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' " +
            "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.TextOutputFormat' " +
            "LOCATION 'oss://example-bucket/synthetic-format-3/' " +
            "TBLPROPERTIES ('comment'='表描述');");
        request.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);
        DdlTableResult reverse = ddlGenerator.reverse(request);
        Table table = reverse.getTable();

        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setAfter(table);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertEquals("CREATE EXTERNAL TABLE IF NOT EXISTS example_external_table\n" +
            "(\n" +
            "   c1 STRING COMMENT '字段描述1',\n" +
            "   c2 STRING COMMENT '字段描述2',\n" +
            "   c3 STRING COMMENT '字段描述3'\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.TextOutputFormat'\n" +
            "LOCATION 'oss://example-bucket/synthetic-format-3/'\n;" ,
            generatorResult.getDialectNodes().get(0).getNode());
        String generated = generatorResult.getDialectNodes().get(0).getNode();
        Assert.assertTrue(generatorResult.getDialectNodes().get(0).getExecutable());
        request.setCode(generated);
        Table roundTripped = ddlGenerator.reverse(request).getTable();
        Assert.assertEquals("example_external_table", roundTripped.getName());
        Assert.assertEquals(3, roundTripped.getColumns().size());
        modelRequest.setAfter(roundTripped);
        Assert.assertEquals(generated, ddlGenerator.generate(modelRequest).getDialectNodes().get(0).getNode());
    }

    @Test
    public void testDiffCreateExternalTableDdl1() {
        // 内置文本数据解析器
        String beforeDdl = "create external table mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.CsvStorageHandler' \n" +
            "with serdeproperties (\n" +
            " 'odps.properties.rolearn'='acs:ram::xxxxxx:role/aliyunodpsdefaultrole'\n" +
            ") \n" +
            "location 'oss://example-bucket/synthetic-format-1/';";
        DdlReverseSqlRequest beforeRequest = new DdlReverseSqlRequest();
        beforeRequest.setCode(beforeDdl);
        beforeRequest.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);
        Table beforeTable = ddlGenerator.reverse(beforeRequest).getTable();
        String afterDdl = "create external table mc_oss_csv_external3\n" +
            "(\n" +
            "vehicleId int,\n" +
            "locationLatitute double,\n" +
            "recordTime string\n" +
            ")\n" +
            "stored by 'com.aliyun.odps.CsvStorageHandler' \n" +
            "with serdeproperties (\n" +
            " 'odps.properties.rolearn'='acs:ram::xxxxxx:role/aliyunodpsdefaultrole',\n" +
            " 'odps.text.option.gzip.input.enabled'='true',\n" +
            " 'odps.text.option.gzip.output.enabled'='true' \n" +
            ") \n" +
            "location 'oss://example-bucket/synthetic-format-1/';";
        DdlReverseSqlRequest afterRequest = new DdlReverseSqlRequest();
        afterRequest.setCode(afterDdl);
        afterRequest.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);
        Table afterTable = ddlGenerator.reverse(afterRequest).getTable();

        DdlGeneratorModelRequest modelRequest = new DdlGeneratorModelRequest();
        modelRequest.setBefore(beforeTable);
        modelRequest.setAfter(afterTable);
        modelRequest.setConfig(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build());
        DdlGeneratorResult generatorResult = ddlGenerator.generate(modelRequest);
        Assert.assertEquals(false, generatorResult.getDialectNodes().get(0).getExecutable());
        Assert.assertEquals(
            "ALTER TABLE mc_oss_csv_external3 SET PROPERTIES('serde_props.odps.text.option.gzip.input.enabled'='true','serde_props.odps.text.option"
                + ".gzip.output.enabled'='true');",
            generatorResult.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGenerateDdl() {
        Table table = new Table();
        List<Column> columns = new ArrayList<>();
        Column column = new Column();
        column.setName("id");
        column.setDataType("BIGINT");
        table.setColumns(Arrays.asList(column));
        table.setDatabase("project1");
        table.setName("tbl1");
        // 属性
        List<BaseClientProperty> baseClientProperties = new ArrayList<>();
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("table.format.version");
        stringProperty.setValueString("2");
        baseClientProperties.add(stringProperty);

        stringProperty = new StringProperty();
        stringProperty.setKey("acid.data.retain.hours");
        stringProperty.setValueString("1");
        baseClientProperties.add(stringProperty);

        table.setProperties(baseClientProperties);
        Constraint constraint = new Constraint();
        constraint.setType(OutlineConstraintType.PRIMARY_KEY);
        constraint.setColumns(Arrays.asList("id"));
        table.setConstraints(Collections.singletonList(constraint));

        // 表配置
        TableConfig tableConfig = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();

        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorSqlRequest ddlGeneratorSqlRequest = DdlGeneratorSqlRequest.builder()
            .after(table)
            .config(tableConfig)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(ddlGeneratorSqlRequest);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        String sql = buildSql(dialectNodes);
        Assert.assertNotNull(sql);
        DdlReverseSqlRequest reverseRequest = new DdlReverseSqlRequest();
        reverseRequest.setCode(sql);
        reverseRequest.setDialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE);
        Table reversed = codeGenerator.reverse(reverseRequest).getTable();
        Assert.assertEquals("tbl1", reversed.getName());
        Assert.assertEquals(1, reversed.getColumns().size());
        Assert.assertEquals("id", reversed.getColumns().get(0).getName());
        Assert.assertEquals("BIGINT", reversed.getColumns().get(0).getDataType());
        Assert.assertEquals(1, reversed.getConstraints().size());
        Assert.assertEquals(OutlineConstraintType.PRIMARY_KEY, reversed.getConstraints().get(0).getType());
        Assert.assertEquals(Collections.singletonList("id"), reversed.getConstraints().get(0).getColumns());
        Assert.assertEquals("2", reversed.getProperties().stream()
            .filter(property -> "table.format.version".equals(property.getKey()))
            .findFirst().get().valueString());
        Assert.assertEquals("1", reversed.getProperties().stream()
            .filter(property -> "acid.data.retain.hours".equals(property.getKey()))
            .findFirst().get().valueString());
    }

    public static String buildSql(List<DialectNode> dialectNodes) {
        List<DialectNode> list = dialectNodes.stream()
            .filter(d -> d.isExecutable())
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(list)) {
            StringBuffer stringBuffer = new StringBuffer();
            for (DialectNode dialectNode : list) {
                stringBuffer.append(dialectNode.getNode()).append("\n");
            }
            return stringBuffer.toString();
        }
        return null;
    }

}
