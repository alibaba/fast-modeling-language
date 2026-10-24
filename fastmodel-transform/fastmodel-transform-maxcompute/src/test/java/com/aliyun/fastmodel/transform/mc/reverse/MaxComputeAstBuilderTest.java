/*
 * Copyright 2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.reverse;

import com.aliyun.fastmodel.core.exception.SemanticException;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.MaxComputeTransformer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * reverse builder
 *
 * @author panguanjing
 * @date 2022/4/12
 */
public class MaxComputeAstBuilderTest {

    MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testReverse() {
        String code = "create table a (s bigint, d string) comment 'abc';";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        String actual = reverse.toString();
        MatcherAssert.assertThat(actual, Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s BIGINT,\n"
            + "   d STRING,\n"
            + "   PRIMARY KEY(s,d)\n"
            + ")\n"
            + "COMMENT 'abc'"));
        BaseStatement baseStatement = nodeParser.parseStatement(actual);
        assertNotNull(baseStatement);
    }

    @Test
    public void testReversePartition() {
        String code = "create table a (s bigint, d string) comment 'abc' partitioned by (ds string);";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("ds")).primary(true).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        String actual = reverse.toString();
        MatcherAssert.assertThat(actual, Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s BIGINT,\n"
            + "   d STRING,\n"
            + "   PRIMARY KEY(s,d,ds)\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   ds STRING\n"
            + ")"));
        BaseStatement baseStatement = nodeParser.parseStatement(actual);
        assertNotNull(baseStatement);
    }

    @Test(expected = SemanticException.class)
    public void testReversePartitionWithSameColumn() {
        String code = "create table a (s bigint, d string) comment 'abc' partitioned by (s string);";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).primary(true).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).primary(true).build())
            .build();
        maxComputeTransformer.reverse(new DialectNode(code), context);
    }

    @Test
    public void testReverseAlias() {
        String code = "create table a (s bigint, d string) comment 'abc';";
        ReverseContext context = ReverseContext.builder()
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("s")).aliasedName(new AliasedName("ddd")).build())
            .columnProperty(ColumnDefinition.builder().colName(new Identifier("d")).build())
            .build();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), context);
        MatcherAssert.assertThat(reverse.toString(), Matchers.equalTo("CREATE DIM TABLE a \n"
            + "(\n"
            + "   s ALIAS 'ddd' BIGINT,\n"
            + "   d STRING\n"
            + ")\n"
            + "COMMENT 'abc'"));
    }

    @Test
    public void testReverseTblProperties() {
        String code = "create table uttable (pk bigint not null, pk2 bigint not null, val bigint, val2 bigint, primary key (pk, pk2)) tblproperties"
            + " (\"transactional\"=\"true\", \"write.bucket.num\" = \"64\", \"acid.data.retain.hours\"=\"120\") lifecycle 7;";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        MatcherAssert.assertThat(reverse.toString(), Matchers.equalTo("CREATE DIM TABLE uttable \n"
            + "(\n"
            + "   pk   BIGINT NOT NULL,\n"
            + "   pk2  BIGINT NOT NULL,\n"
            + "   val  BIGINT,\n"
            + "   val2 BIGINT,\n"
            + "   PRIMARY KEY(pk,pk2)\n"
            + ")\n"
            + "WITH('transactional'='true','write.bucket.num'='64','acid.data.retain.hours'='120','life_cycle'='7')"));
    }

    @Test
    public void testReverseUtTable() {
        String code = "create table uttable (pk bigint not null primary key, val bigint) tblproperties (\"transactional\"=\"true\");\n";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        MatcherAssert.assertThat(reverse.toString(), Matchers.equalTo("CREATE DIM TABLE uttable \n"
            + "(\n"
            + "   pk  BIGINT NOT NULL,\n"
            + "   val BIGINT,\n"
            + "   PRIMARY KEY(pk)\n"
            + ")\n"
            + "WITH('transactional'='true')"));
    }

    @Test
    public void testReverseTblPropertiesForOpenCSV() {
        String code = "CREATE EXTERNAL TABLE ext_csv_test08\n" +
            "(\n" +
            "  vehicleId INT,\n" +
            "  recordId INT,\n" +
            "  patientId INT,\n" +
            "  calls INT,\n" +
            "  locationLatitute DOUBLE,\n" +
            "  locationLongtitue DOUBLE,\n" +
            "  recordTime STRING,\n" +
            "  direction STRING\n" +
            ")\n" +
            "ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
            "WITH serdeproperties (\n" +
            "\"separatorChar\" = \",\"\n" +
            ")\n" +
            "stored AS textfile\n" +
            "location 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-1/'\n" +
            "-- 设置忽略首行和尾行参数。\n" +
            "TBLPROPERTIES (\n" +
            "\"skip.header.line.COUNT\"=\"1\",\n" +
            "\"skip.footer.line.COUNT\"=\"1\"\n" +
            ")\n" +
            ";";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        Table table = maxComputeTransformer.transformTable(reverse, TransformContext.builder().build());
        Node node = maxComputeTransformer.reverseTable(table, ReverseContext.builder().build());
        DialectNode transform = maxComputeTransformer.transform((BaseStatement) node, TransformContext.builder().filterProperties(false).build());
        MatcherAssert.assertThat(transform.getNode(), Matchers.equalTo("CREATE EXTERNAL TABLE ext_csv_test08\n" +
            "(\n" +
            "   vehicleid         INT,\n" +
            "   recordid          INT,\n" +
            "   patientid         INT,\n" +
            "   calls             INT,\n" +
            "   locationlatitute  DOUBLE,\n" +
            "   locationlongtitue DOUBLE,\n" +
            "   recordtime        STRING,\n" +
            "   direction         STRING\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'separatorChar' = ','\n" +
            ")\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-1/'\n" +
            "TBLPROPERTIES ('skip.header.line.COUNT'='1','skip.footer.line.COUNT'='1')"));
    }

    @Test
    public void testReverseTblPropertiesForTextFile() {
        String code = "CREATE EXTERNAL TABLE external_tb_textfile\n" +
            "(\n" +
            "  vehicleId INT,\n" +
            "  recordId INT,\n" +
            "  patientId INT,\n" +
            "  calls INT,\n" +
            "  locationLatitute DOUBLE,\n" +
            "  locationLongtitue DOUBLE,\n" +
            "  recordTime STRING,\n" +
            "  direction STRING\n" +
            ")\n" +
            "row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
            "WITH serdeproperties (\n" +
            "    'odps.properties.rolearn'='acs:ram::<uid>:role/aliyunodpsdefaultrole')\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-4/'\n" +
            "tblproperties (\n" +
            "    'mcfed.mapreduce.output.fileoutputformat.compress'='True',\n" +
            "    'mcfed.mapreduce.output.fileoutputformat.compress.codec'='org.apache.hadoop.io.compress.SnappyCodec')\n" +
            ";";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        Table table = maxComputeTransformer.transformTable(reverse, TransformContext.builder().build());
        Node node = maxComputeTransformer.reverseTable(table, ReverseContext.builder().build());
        DialectNode transform = maxComputeTransformer.transform((BaseStatement) node, TransformContext.builder().filterProperties(false).build());
        MatcherAssert.assertThat(transform.getNode(), Matchers.equalTo("CREATE EXTERNAL TABLE external_tb_textfile\n" +
            "(\n" +
            "   vehicleid         INT,\n" +
            "   recordid          INT,\n" +
            "   patientid         INT,\n" +
            "   calls             INT,\n" +
            "   locationlatitute  DOUBLE,\n" +
            "   locationlongtitue DOUBLE,\n" +
            "   recordtime        STRING,\n" +
            "   direction         STRING\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'odps.properties.rolearn' = 'acs:ram::<uid>:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-4/'\n" +
            "TBLPROPERTIES ('mcfed.mapreduce.output.fileoutputformat.compress'='True','mcfed.mapreduce.output.fileoutputformat.compress.codec'='org.apache.hadoop.io.compress.SnappyCodec')"));
    }

    @Test
    public void testReverseTblPropertiesForPaimon() {
        String code = "CREATE EXTERNAL TABLE oss_extable_paimon_pt\n" +
            "(\n" +
            "    id BIGINT,\n" +
            "    data STRING\n" +
            ")\n" +
            "PARTITIONED BY (dt STRING )\n" +
            "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'\n" +
            "WITH serdeproperties (\n" +
            "    'odps.properties.rolearn'='acs:ram::<uid>:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-2/'\n" +
            ";";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        Table table = maxComputeTransformer.transformTable(reverse, TransformContext.builder().build());
        Node node = maxComputeTransformer.reverseTable(table, ReverseContext.builder().build());
        DialectNode transform = maxComputeTransformer.transform((BaseStatement) node, TransformContext.builder().build());
        MatcherAssert.assertThat(transform.getNode(), Matchers.equalTo("CREATE EXTERNAL TABLE oss_extable_paimon_pt\n" +
            "(\n" +
            "   id   BIGINT,\n" +
            "   `data` STRING\n" +
            ")\n" +
            "PARTITIONED BY\n" +
            "(\n" +
            "   dt STRING\n" +
            ")\n" +
            "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'odps.properties.rolearn' = 'acs:ram::<uid>:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-2/'\n"));
    }

    @Test
    public void testReverseTblPropertiesForJson() {
        String code = "CREATE EXTERNAL TABLE mc_oss_extable_name_json\n" +
            "(\n" +
            "  action STRING,\n" +
            "  time STRING\n" +
            ")\n" +
            "PARTITIONED BY (dt STRING)\n" +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "WITH serdeproperties (\n" +
            "  'odps.properties.rolearn'='acs:ram::<uid>:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-3/';";
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        Table table = maxComputeTransformer.transformTable(reverse, TransformContext.builder().build());
        Node node = maxComputeTransformer.reverseTable(table, ReverseContext.builder().build());
        DialectNode transform = maxComputeTransformer.transform((BaseStatement) node, TransformContext.builder().build());
        MatcherAssert.assertThat(transform.getNode(), Matchers.equalTo("CREATE EXTERNAL TABLE mc_oss_extable_name_json\n" +
            "(\n" +
            "   action STRING,\n" +
            "   time   STRING\n" +
            ")\n" +
            "PARTITIONED BY\n" +
            "(\n" +
            "   dt STRING\n" +
            ")\n" +
            "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "   'odps.properties.rolearn' = 'acs:ram::<uid>:role/aliyunodpsdefaultrole'\n" +
            ")\n" +
            "STORED AS textfile\n" +
            "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-format-3/'\n"));
    }

    @Test
    public void externalTableInputOutputFormatsRoundTrip() {
        String sql = "CREATE EXTERNAL TABLE example_external_formats (example_id BIGINT) "
            + "STORED AS INPUTFORMAT 'org.example.InputFormat' OUTPUTFORMAT 'org.example.OutputFormat' "
            + "LOCATION 'oss://oss-cn-hangzhou.aliyuncs.com/example-bucket/synthetic-formats/';";
        BaseStatement statement = maxComputeTransformer.reverse(new DialectNode(sql));
        TransformContext context = TransformContext.builder().filterProperties(false).build();
        DialectNode generated = maxComputeTransformer.transform(statement, context);
        assertTrue(generated.isExecutable());
        assertTrue(generated.getNode().contains("INPUTFORMAT 'org.example.InputFormat'"));
        assertTrue(generated.getNode().contains("OUTPUTFORMAT 'org.example.OutputFormat'"));
        BaseStatement reparsed = maxComputeTransformer.reverse(generated);
        assertEquals(generated.getNode(), maxComputeTransformer.transform(reparsed, context).getNode());
    }
}
