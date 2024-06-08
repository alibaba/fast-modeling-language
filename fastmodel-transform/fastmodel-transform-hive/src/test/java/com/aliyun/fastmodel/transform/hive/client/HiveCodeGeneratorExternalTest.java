/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.client;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.client.property.FieldsTerminated;
import com.aliyun.fastmodel.transform.hive.client.property.LinesTerminated;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat.StorageFormatEnum;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class HiveCodeGeneratorExternalTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        List<BaseClientProperty> properties = Lists.newArrayList();
        StorageFormat storageFormat = new StorageFormat();
        storageFormat.setValue(StorageFormatEnum.ORC);
        properties.add(storageFormat);

        FieldsTerminated fieldsTerminated = new FieldsTerminated();
        fieldsTerminated.setValue("\\001");
        properties.add(fieldsTerminated);

        LinesTerminated linesTerminated = new LinesTerminated();
        linesTerminated.setValue("\\n");
        properties.add(linesTerminated);

        table.setProperties(properties);
        table.setExternal(true);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HIVE)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        DialectNode dialectNode1 = dialectNodes.get(0);
        assertEquals(dialectNode1.getNode(), "CREATE EXTERNAL TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1 STRING COMMENT 'comment'\n"
            + ")\n"
            + "ROW FORMAT DELIMITED\n"
            + "FIELDS TERMINATED BY '\\001'\n"
            + "LINES TERMINATED BY '\\n'\n"
            + "STORED AS ORC;");

        config.setAppendSemicolon(false);
        generate = codeGenerator.generate(request);
        dialectNodes = generate.getDialectNodes();
        DialectNode dialectNode = dialectNodes.get(0);
        String node = dialectNode.getNode();
        assertEquals(node, "CREATE EXTERNAL TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1 STRING COMMENT 'comment'\n"
            + ")\n"
            + "ROW FORMAT DELIMITED\n"
            + "FIELDS TERMINATED BY '\\001'\n"
            + "LINES TERMINATED BY '\\n'\n"
            + "STORED AS ORC");
    }

    @Test
    public void testGeneratorHiveTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        String createTableDdl = "CREATE TABLE `default.test_ziliang_v1`(\n" +
                "  `a` string COMMENT 'aa1')\n" +
                "COMMENT '123'\n" +
                "PARTITIONED BY ( \n" +
                "  `ds` string COMMENT '')\n" +
                "ROW FORMAT SERDE \n" +
                "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n" +
                "STORED AS INPUTFORMAT \n" +
                "  'org.apache.hadoop.mapred.TextInputFormat' \n" +
                "OUTPUTFORMAT \n" +
                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION\n" +
                "  'hdfs://master-1-1.c-aac6b952af7280c8.cn-shanghai.emr.aliyuncs.com:9000/user/hive/warehouse/test_ziliang_v1'\n" +
                "TBLPROPERTIES (\n" +
                "  'bucketing_version'='2', \n" +
                "  'transient_lastDdlTime'='1698806349')";
        DialectNode dialectNode = new DialectNode(createTableDdl);
        ReverseContext build = ReverseContext.builder().build();
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getHive());
        Node reverse = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(reverse, TransformContext.builder().build());


        TableConfig config = TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_HIVE)
                .caseSensitive(false)
                .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(config)
                .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        DialectNode dialectNode1 = dialectNodes.get(0);
        assertEquals(dialectNode1.getNode(), "CREATE TABLE `default.test_ziliang_v1`\n" +
                "(\n" +
                "   a STRING COMMENT 'aa1'\n" +
                ")\n" +
                "COMMENT '123'\n" +
                "PARTITIONED BY\n" +
                "(\n" +
                "   ds STRING COMMENT ''\n" +
                ")\n" +
                "ROW FORMAT SERDE\n" +
                "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                "STORED AS \n" +
                "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION 'hdfs://master-1-1.c-aac6b952af7280c8.cn-shanghai.emr.aliyuncs.com:9000/user/hive/warehouse/test_ziliang_v1'\n" +
                "TBLPROPERTIES ('bucketing_version'='2','transient_lastDdlTime'='1698806349','hive.row_format_serde'='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe','hive.stored_input_format'='org.apache.hadoop.mapred.TextInputFormat','hive.stored_output_format'='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat','hive.location'='hdfs://master-1-1.c-aac6b952af7280c8.cn-shanghai.emr.aliyuncs.com:9000/user/hive/warehouse/test_ziliang_v1');");
    }

    @Test
    public void testGenerateHiveExternalTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        String createTableDdl = "CREATE EXTERNAL TABLE `oss_share_feedback`(\n" +
                "  `uid` string, \n" +
                "  `os` string, \n" +
                "  `source_id` string, \n" +
                "  `type` string, \n" +
                "  `target_key` string, \n" +
                "  `created_time` string, \n" +
                "  `updated_time` string)\n" +
                "PARTITIONED BY ( \n" +
                "  `pt_month` string, \n" +
                "  `pt_day` string)\n" +
                "ROW FORMAT SERDE \n" +
                "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n" +
                "WITH SERDEPROPERTIES ( \n" +
                "  'field.delim'='\\t', \n" +
                "  'line.delim'='\\n', \n" +
                "  'serialization.format'='\\t') \n" +
                "STORED AS INPUTFORMAT \n" +
                "  'org.apache.hadoop.mapred.TextInputFormat' \n" +
                "OUTPUTFORMAT \n" +
                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION\n" +
                "  'oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'\n" +
                "TBLPROPERTIES (\n" +
                "  'transient_lastDdlTime'='1495603307');";
        DialectNode dialectNode = new DialectNode(createTableDdl);
        ReverseContext build = ReverseContext.builder().build();
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getHive());
        Node reverse = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(reverse, TransformContext.builder().build());


        TableConfig config = TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_HIVE)
                .caseSensitive(false)
                .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(config)
                .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        DialectNode dialectNode1 = dialectNodes.get(0);
        assertEquals(dialectNode1.getNode(), "CREATE EXTERNAL TABLE oss_share_feedback\n" +
                "(\n" +
                "   uid          STRING,\n" +
                "   os           STRING,\n" +
                "   source_id    STRING,\n" +
                "   type         STRING,\n" +
                "   target_key   STRING,\n" +
                "   created_time STRING,\n" +
                "   updated_time STRING\n" +
                ")\n" +
                "PARTITIONED BY\n" +
                "(\n" +
                "   pt_month STRING,\n" +
                "   pt_day   STRING\n" +
                ")\n" +
                "ROW FORMAT SERDE\n" +
                "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
                "WITH SERDEPROPERTIES (\n" +
                "   'field.delim' = '\\t',\n" +
                "   'line.delim' = '\\n',\n" +
                "   'serialization.format' = '\\t'\n" +
                ")\n" +
                "STORED AS \n" +
                "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION 'oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'\n" +
                "TBLPROPERTIES ('transient_lastDdlTime'='1495603307','hive.table_external'='true'," +
                "'hive.row_format_serde'='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'," +
                "'hive.serde_props.field.delim'='\\t','hive.serde_props.line.delim'='\\n'," +
                "'hive.serde_props.serialization.format'='\\t'," +
                "'hive.stored_input_format'='org.apache.hadoop.mapred.TextInputFormat'," +
                "'hive.stored_output_format'='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'," +
                "'hive.location'='oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'," +
                "'hive.table_external'='true');");
    }

    @Test
    public void testGenerateHiveExternalTable2() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        String createTableDdl = "CREATE EXTERNAL TABLE `oss_share_feedback`(\n" +
                "  `uid` string, \n" +
                "  `os` string, \n" +
                "  `source_id` string, \n" +
                "  `type` string, \n" +
                "  `target_key` string, \n" +
                "  `created_time` string, \n" +
                "  `updated_time` string)\n" +
                "PARTITIONED BY ( \n" +
                "  `pt_month` string, \n" +
                "  `pt_day` string)\n" +
                "ROW FORMAT delimited \n" +
                "fields terminated by ','" +
                "STORED AS INPUTFORMAT \n" +
                "  'org.apache.hadoop.mapred.TextInputFormat' \n" +
                "OUTPUTFORMAT \n" +
                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION\n" +
                "  'oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'\n" +
                "TBLPROPERTIES (\n" +
                "  'transient_lastDdlTime'='1495603307');";
        DialectNode dialectNode = new DialectNode(createTableDdl);
        ReverseContext build = ReverseContext.builder().build();
        Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.getHive());
        Node reverse = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(reverse, TransformContext.builder().build());


        TableConfig config = TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_HIVE)
                .caseSensitive(false)
                .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(config)
                .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        DialectNode dialectNode1 = dialectNodes.get(0);
        assertEquals(dialectNode1.getNode(), "CREATE EXTERNAL TABLE oss_share_feedback\n" +
                "(\n" +
                "   uid          STRING,\n" +
                "   os           STRING,\n" +
                "   source_id    STRING,\n" +
                "   type         STRING,\n" +
                "   target_key   STRING,\n" +
                "   created_time STRING,\n" +
                "   updated_time STRING\n" +
                ")\n" +
                "PARTITIONED BY\n" +
                "(\n" +
                "   pt_month STRING,\n" +
                "   pt_day   STRING\n" +
                ")\n" +
                "ROW FORMAT DELIMITED\n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS \n" +
                "INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n" +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION 'oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'\n" +
                "TBLPROPERTIES ('transient_lastDdlTime'='1495603307','hive.table_external'='true'," +
                "'hive.fields_terminated'=',','hive.stored_input_format'='org.apache.hadoop.mapred.TextInputFormat'," +
                "'hive.stored_output_format'='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'," +
                "'hive.location'='oss://{AccessKeyId}:{AccessKeySecret}@{bucket}.{endpoint}/hive/oss_share_feedback'," +
                "'hive.table_external'='true');");
    }

    @Test
    public void testGenerator1() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        String ddlString
            = "CREATE TABLE IF NOT EXISTS default.hudi_test_8\n(\n   id    INT COMMENT '',\n   col1  STRING COMMENT '',\n   col2  STRING COMMENT "
            + "'',\n   col3  STRING COMMENT '',\n   col4  STRING COMMENT '',\n   col5  STRING COMMENT '',\n   col6  STRING COMMENT '',\n   col7  "
            + "STRING COMMENT '',\n   col8  STRING COMMENT '',\n   col9  STRING COMMENT '',\n   col10 STRING COMMENT ''\n)\nCOMMENT ''\nPARTITIONED"
            + " BY\n(\n   ds STRING COMMENT 'test'\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '\\001'\nLINES TERMINATED BY '\\n'\nSTORED AS "
            + "ORC;\n";
        DdlReverseSqlRequest request = DdlReverseSqlRequest.builder()
            .database("default")
            .schema(null)
            .code(ddlString)
            .dialectMeta(DialectMeta.DEFAULT_HIVE).build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        Table table = reverse.getTable();
        assertEquals(table.getDatabase(), "default");
    }

    @Test
    public void testGeneratorKeyword() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HIVE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").build(),
            Column.builder().name("int").dataType("int").build(),
            Column.builder().name("decimal").dataType("decimal").precision(18).length(18).build()
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertEquals(dialectNodes.get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   `double` BIGINT,\n"
            + "   `bigint` BIGINT,\n"
            + "   `int`   INT,\n"
            + "   `decimal` DECIMAL(18)\n"
            + ");");
    }

    @Test
    public void testGeneratorTableKeyWord() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HIVE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").build(),
            Column.builder().name("int").dataType("int").build(),
            Column.builder().name("decimal").dataType("decimal").precision(18).length(18).build()
        );
        List<String> keyWordTable = Lists.newArrayList("__abc__");
        for (String w : keyWordTable) {
            Table table = Table.builder()
                .name(w)
                .columns(columns)
                .build();
            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(config)
                .build();
            DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
            List<DialectNode> dialectNodes = reverse.getDialectNodes();
            assertEquals(dialectNodes.get(0).getNode(), "CREATE TABLE IF NOT EXISTS `__abc__`\n"
                + "(\n"
                + "   `double` BIGINT,\n"
                + "   `bigint` BIGINT,\n"
                + "   `int`   INT,\n"
                + "   `decimal` DECIMAL(18)\n"
                + ");");
        }

    }

    @Test
    public void testGeneratorTableKeyWordDot() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HIVE)
            .caseSensitive(false)
            .build();
        List<Column> columns = Lists.newArrayList(
            Column.builder().name("double").dataType("bigint").build(),
            Column.builder().name("bigint").dataType("bigint").build(),
            Column.builder().name("int").dataType("int").build(),
            Column.builder().name("decimal").dataType("decimal").precision(18).length(18).build()
        );
        List<String> keyWordTable = Lists.newArrayList("bcd.abc");
        for (String w : keyWordTable) {
            Table table = Table.builder()
                .name(w)
                .columns(columns)
                .build();
            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(config)
                .build();
            DdlGeneratorResult reverse = defaultCodeGenerator.generate(request);
            List<DialectNode> dialectNodes = reverse.getDialectNodes();
            assertEquals(dialectNodes.get(0).getNode(), "CREATE TABLE IF NOT EXISTS `bcd.abc`\n"
                + "(\n"
                + "   `double` BIGINT,\n"
                + "   `bigint` BIGINT,\n"
                + "   `int`   INT,\n"
                + "   `decimal` DECIMAL(18)\n"
                + ");");
        }

    }

    private Table getTable(String id, String name, long lifeCycleSeconds) {
        List<Column> columns = getColumns(id, name);
        return Table.builder()
            .name("a")
            .columns(columns)
            .lifecycleSeconds(lifeCycleSeconds)
            .build();
    }

    private List<Column> getColumns(String id, String name) {
        return Lists.newArrayList(
            Column.builder()
                .id(id)
                .name(name)
                .dataType("string")
                .comment("comment")
                .build()
        );
    }

}
