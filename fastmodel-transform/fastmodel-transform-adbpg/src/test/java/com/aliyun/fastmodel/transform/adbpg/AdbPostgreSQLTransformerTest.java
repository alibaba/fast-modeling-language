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

package com.aliyun.fastmodel.transform.adbpg;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.AdbPostgreSQLPartitionBy;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/16
 */
public class AdbPostgreSQLTransformerTest {

    AdbPostgreSQLTransformer transformer = new AdbPostgreSQLTransformer();

    @Test
    @SneakyThrows
    public void testParse() {
        String t = IOUtils.resourceToString("/adbpostgresql/sub_partition_by.txt", Charset.defaultCharset());
        BaseStatement reverse = transformer.reverse(new DialectNode(t));
        CreateTable createTable = (CreateTable)reverse;
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertEquals(AdbPostgreSQLPartitionBy.class, partitionedBy.getClass());
    }

    @Test
    public void testTransform() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("bigint", null)).build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .build();
        TransformContext context = AdbPostgreSQLTransformContext.builder().build();
        DialectNode transform = transformer.transform(source, context);
        assertEquals("CREATE TABLE abc (\n"
            + "   c1 BIGINT\n"
            + ");", transform.getNode());
    }

    @Test
    @SneakyThrows
    public void testGeneratorBasic() {
        String sql = IOUtils.resourceToString("/adbpostgresql/basic.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String generator = generator(dialectNode);
        assertEquals("CREATE TABLE baby.rank (\n"
            + "   id     INTEGER,\n"
            + "   rank   INTEGER,\n"
            + "   year   SMALLINT,\n"
            + "   gender CHAR(1),\n"
            + "   count  INTEGER\n"
            + ")\n"
            + "DISTRIBUTED BY (rank,gender,year)\n"
            + ";", generator);
    }

    @Test
    @SneakyThrows
    public void testGeneratorDefault() {
        String sql = IOUtils.resourceToString("/adbpostgresql/default.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String generator = generator(dialectNode);
        assertEquals("CREATE TABLE distributors (\n"
            + "   did  INTEGER PRIMARY KEY DEFAULT nextval('serial'),\n"
            + "   name VARCHAR(40) NOT NULL CHECK (name <> '')\n"
            + ");", generator);
    }

    @Test
    @SneakyThrows
    public void testGeneratorFilms() {
        String sql = IOUtils.resourceToString("/adbpostgresql/films.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String generator = generator(dialectNode);
        assertEquals("CREATE TABLE films (\n"
            + "   code      CHAR(5) PRIMARY KEY,\n"
            + "   title     VARCHAR(40) NOT NULL,\n"
            + "   did       INTEGER NOT NULL,\n"
            + "   date_prod DATE,\n"
            + "   kind      VARCHAR(10),\n"
            + "   len       INTERVAL HOUR TO MINUTE\n"
            + ");", generator);
    }

    @Test
    @SneakyThrows
    public void testGeneratorPartitionBy() {
        String sql = IOUtils.resourceToString("/adbpostgresql/partition_by.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String generator = generator(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY LIST (code)\n"
            + "(PARTITION sales VALUES ('S')\n"
            + ",PARTITION returns VALUES ('R'));", generator);
    }

    @SneakyThrows
    @Test
    public void testGeneratorWithPartitionBy() {
        String sql = IOUtils.resourceToString("/adbpostgresql/partition_by.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String table = generatorWithoutTable(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY LIST (code)\n"
            + "(PARTITION sales VALUES ('S')\n"
            + ",PARTITION returns VALUES ('R'));", table);
    }

    @SneakyThrows
    @Test
    public void testGeneratorWithPartitionBySubPartition() {
        String sql = IOUtils.resourceToString("/adbpostgresql/sub_partition_by.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String table = generatorWithoutTable(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY LIST (code)\n"
            + "SUBPARTITION BY RANGE (c_rank)\n"
            + "SUBPARTITION BY LIST (region)\n"
            + "(PARTITION sales VALUES ('S')\n"
            + ",PARTITION returns VALUES ('R'));", table);
    }

    @SneakyThrows
    @Test
    public void testGeneratorWithPartitionWithTableBySubPartition() {
        String sql = IOUtils.resourceToString("/adbpostgresql/sub_partition_by.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String table = generator(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY LIST (code)\n"
            + "SUBPARTITION BY RANGE (c_rank)\n"
            + "SUBPARTITION BY LIST (region)\n"
            + "(PARTITION sales VALUES ('S')\n"
            + ",PARTITION returns VALUES ('R'));", table);
    }

    @SneakyThrows
    @Test
    public void testGeneratorWithPartitionBySubPartitionTemplate() {
        String sql = IOUtils.resourceToString("/adbpostgresql/sub_partition_with_template.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String table = generatorWithoutTable(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY RANGE (year)\n"
            + "SUBPARTITION BY RANGE (qtr) SUBPARTITION TEMPLATE \n"
            + "(START (1)END (5)EVERY (1),DEFAULT SUBPARTITION bad_qtr)\n"
            + "SUBPARTITION BY LIST (region) SUBPARTITION TEMPLATE (SUBPARTITION usa VALUES ('usa'),SUBPARTITION europe VALUES ('europe'),"
            + "SUBPARTITION asia VALUES ('asia'),DEFAULT SUBPARTITION other_regions)\n"
            + "(START (2009)END (2011)EVERY (1)\n"
            + ",DEFAULT PARTITION outlying_years);", table);
    }

    @SneakyThrows
    @Test
    public void testGeneratorPartitionBySubPartitionTemplate() {
        String sql = IOUtils.resourceToString("/adbpostgresql/sub_partition_with_template.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(sql);
        String table = generator(dialectNode);
        assertEquals("CREATE TABLE sales (\n"
            + "   id     INTEGER,\n"
            + "   year   INTEGER,\n"
            + "   qtr    INTEGER,\n"
            + "   c_rank INTEGER,\n"
            + "   code   CHAR(1),\n"
            + "   region TEXT\n"
            + ")\n"
            + "DISTRIBUTED BY (id)\n"
            + "PARTITION BY RANGE (year)\n"
            + "SUBPARTITION BY RANGE (qtr) SUBPARTITION TEMPLATE \n"
            + "(START (1)END (5)EVERY (1),DEFAULT SUBPARTITION bad_qtr)\n"
            + "SUBPARTITION BY LIST (region) SUBPARTITION TEMPLATE (SUBPARTITION usa VALUES ('usa'),SUBPARTITION europe VALUES ('europe'),"
            + "SUBPARTITION asia VALUES ('asia'),DEFAULT SUBPARTITION other_regions)\n"
            + "(START (2009)END (2011)EVERY (1)\n"
            + ",DEFAULT PARTITION outlying_years);", table);
    }

    private String generatorWithoutTable(DialectNode dialectNode) {
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = transformer.reverse(dialectNode, build);
        return transformer.transform((BaseStatement)node, TransformContext.builder().build()).getNode();
    }

    private String generator(DialectNode dialectNode) {
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(node, TransformContext.builder().build());
        Node reverseNode = transformer.reverseTable(table);
        return transformer.transform((BaseStatement)reverseNode, TransformContext.builder().build()).getNode();
    }
}