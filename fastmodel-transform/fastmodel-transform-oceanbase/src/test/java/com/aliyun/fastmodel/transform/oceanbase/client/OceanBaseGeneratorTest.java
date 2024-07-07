package com.aliyun.fastmodel.transform.oceanbase.client;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
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
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.oceanbase.BaseOceanbaseTest;
import com.aliyun.fastmodel.transform.oceanbase.OceanBaseMysqlTransformer;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * oceanbase generator test
 *
 * @author panguanjing
 * @date 2024/2/5
 */
public class OceanBaseGeneratorTest extends BaseOceanbaseTest {

    private CodeGenerator codeGenerator = new DefaultCodeGenerator();

    private OceanBaseMysqlTransformer oceanBaseMysqlTransformer = new OceanBaseMysqlTransformer();

    @Test
    public void testGeneratorSimple1() {
        assertGenerator("simple1.txt", "CREATE TABLE tbl1 \n"
            + "(\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   c2 VARCHAR(50),\n"
            + "   PRIMARY KEY(c1)\n"
            + ")");
    }

    @Test
    public void testGeneratorSimple2Index() {
        assertGenerator("simple2_index.txt", "CREATE TABLE tbl2 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT,\n"
            + "   c3 INT,\n"
            + "   INDEX i1 (c2)\n"
            + ")");
    }

    @Test
    public void testGeneratorSimple2Index2() {
        assertGenerator("simple2_index2.txt", "CREATE TABLE tbl3 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   UNIQUE KEY((c1 + c2)),\n"
            + "   INDEX i1 ((c1 + 1))\n"
            + ")");
    }

    @Test
    public void testGeneratorSimple3Hash() {
        assertGenerator("simple3_hash.txt", "CREATE TABLE tbl4 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT\n"
            + ")\n"
            + "PARTITION BY HASH (c1) PARTITIONS 8");
    }

    @Test
    public void testGeneratorSimple3Range() {
        assertGenerator("simple3_range.txt", "CREATE TABLE tbl5 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   c3 INT\n"
            + ")\n"
            + "PARTITION BY RANGE (c1) SUBPARTITION BY KEY (c2,c3) SUBPARTITIONS 5\n"
            + "(PARTITION p0 VALUES LESS THAN (0),\n"
            + "PARTITION p1 VALUES LESS THAN (100))");
    }

    @Test
    public void testGeneratorSimple1Comment() {
        assertGenerator("simple1_comment.txt", "CREATE TABLE tbl1 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 VARCHAR(50)\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITION BY KEY (c1) PARTITIONS 8");
    }

    @Test
    public void testGeneratorCharset() {
        assertGenerator("simple4_charset.txt", "CREATE TABLE tbl6 \n"
            + "(\n"
            + "   c1 VARCHAR(10),\n"
            + "   c2 VARCHAR(10)\n"
            + ")\n"
            + "DEFAULT CHARSET utf8\n"
            + "COLLATE utf8mb4_general_ci");
    }

    @Test
    public void testGeneratorCompress() {
        assertGenerator("simple5_compress.txt", "CREATE TABLE tbl7 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   c3 VARCHAR(64)\n"
            + ")\n"
            + "COMPRESSION='zstd_1.0'\n"
            + "ROW_FORMAT=DYNAMIC\n"
            + "PCTFREE=5");
    }

    @Test
    public void testGeneratorPr() {
        assertGenerator("simple6_pr.txt", "CREATE TABLE tbl8 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT\n"
            + ")\n"
            + "PARALLEL=3");
    }

    //@Test
    //public void testGeneratorAutoIncrement() {
    //    assertGenerator("simple7_autoincrement.txt", "CREATE TABLE tbl5 \n"
    //        + "(\n"
    //        + "   c1 INT,\n"
    //        + "   c2 INT,\n"
    //        + "   c3 INT\n"
    //        + ")\n"
    //        + "PARTITION BY RANGE (c1) SUBPARTITION BY KEY (c2,c3) SUBPARTITIONS 5\n"
    //        + "(PARTITION p0 VALUES LESS THAN (0),\n"
    //        + "PARTITION p1 VALUES LESS THAN (100))");
    //}

    //@Test
    //public void testGeneratorCheck() {
    //    assertGenerator("simple8_check.txt", "CREATE TABLE tbl5 \n"
    //        + "(\n"
    //        + "   c1 INT,\n"
    //        + "   c2 INT,\n"
    //        + "   c3 INT\n"
    //        + ")\n"
    //        + "PARTITION BY RANGE (c1) SUBPARTITION BY KEY (c2,c3) SUBPARTITIONS 5\n"
    //        + "(PARTITION p0 VALUES LESS THAN (0),\n"
    //        + "PARTITION p1 VALUES LESS THAN (100))");
    //}

    //@Test
    //public void testGeneratorReference() {
    //    assertGenerator("simple8_reference.txt", "CREATE TABLE tbl5 \n"
    //        + "(\n"
    //        + "   c1 INT,\n"
    //        + "   c2 INT,\n"
    //        + "   c3 INT\n"
    //        + ")\n"
    //        + "PARTITION BY RANGE (c1) SUBPARTITION BY KEY (c2,c3) SUBPARTITIONS 5\n"
    //        + "(PARTITION p0 VALUES LESS THAN (0),\n"
    //        + "PARTITION p1 VALUES LESS THAN (100))");
    //}

    @Test
    public void testGeneratorPtCombine1() {
        assertGenerator("simple9_pt_combine_1.txt", "CREATE TABLE tb1_m_rcr \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 INT\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS (col1) \n"
            + "SUBPARTITION BY RANGE (col2)\n"
            + "SUBPARTITION TEMPLATE (\n"
            + "SUBPARTITION  mp0 VALUES LESS THAN (3),\n"
            + "SUBPARTITION  mp1 VALUES LESS THAN (6),\n"
            + "SUBPARTITION  mp2 VALUES LESS THAN (9)\n"
            + ")\n"
            + "(PARTITION p0 VALUES LESS THAN (100),\n"
            + "PARTITION p1 VALUES LESS THAN (200),\n"
            + "PARTITION p2 VALUES LESS THAN (300))");
    }

    @Test
    public void testGeneratorPtCombine2() {
        assertGenerator("simple9_pt_combine_2.txt", "CREATE TABLE tbl2_f_llc \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 DATE\n"
            + ")\n"
            + "PARTITION BY LIST (col1) SUBPARTITION BY LIST COLUMNS (col2)\n"
            + "(PARTITION p0 VALUES IN (100)(\n"
            + "SUBPARTITION sp0 VALUES IN ('2021/04/01'),\n"
            + "SUBPARTITION sp1 VALUES IN ('2021/07/01'),\n"
            + "SUBPARTITION sp2 VALUES IN ('2021/10/01'),\n"
            + "SUBPARTITION sp3 VALUES IN ('2022/01/01')\n"
            + "),\n"
            + "PARTITION p1 VALUES IN (200)(\n"
            + "SUBPARTITION sp4 VALUES IN ('2021/04/01'),\n"
            + "SUBPARTITION sp5 VALUES IN ('2021/07/01'),\n"
            + "SUBPARTITION sp6 VALUES IN ('2021/10/01'),\n"
            + "SUBPARTITION sp7 VALUES IN ('2022/01/01')\n"
            + "))");
    }

    @Test
    public void testGeneratorPtCombine3() {
        assertGenerator("simple9_pt_combine_3.txt", "CREATE TABLE tbl3_f_hk \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 VARCHAR(50)\n"
            + ")\n"
            + "PARTITION BY HASH (col1) SUBPARTITION BY KEY (col2)\n"
            + "(PARTITION p1(\n"
            + "   SUBPARTITION sp0,\n"
            + "   SUBPARTITION sp1,\n"
            + "   SUBPARTITION sp2,\n"
            + "   SUBPARTITION sp3\n"
            + "),\n"
            + "PARTITION p2(\n"
            + "   SUBPARTITION sp4,\n"
            + "   SUBPARTITION sp5,\n"
            + "   SUBPARTITION sp6,\n"
            + "   SUBPARTITION sp7\n"
            + "))");
    }

    @Test
    public void testGeneratorHash() {
        assertGenerator("simple9_pt_hash.txt", "CREATE TABLE tbl3_h \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 VARCHAR(50)\n"
            + ")\n"
            + "PARTITION BY HASH (col1) PARTITIONS 2");
    }

    @Test
    public void testGeneratorList() {
        assertGenerator("simple9_pt_list.txt", "CREATE TABLE tbl2_l \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 DATE\n"
            + ")\n"
            + "PARTITION BY LIST (col1)\n"
            + "(PARTITION p0 VALUES IN (100),\n"
            + "PARTITION p1 VALUES IN (200))");
    }

    @Test
    public void testGeneratorPtRange() {
        assertGenerator("simple9_pt_range.txt", "CREATE TABLE tb1_rc \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 INT\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS (col1)\n"
            + "(PARTITION p0 VALUES LESS THAN (100),\n"
            + "PARTITION p1 VALUES LESS THAN (200),\n"
            + "PARTITION p2 VALUES LESS THAN (300))");
    }

    @Test
    public void testGeneratorPtKey() {
        assertGenerator("simple9_pt_key.txt", "CREATE TABLE tb1_rc \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 INT\n"
            + ")\n"
            + "PARTITION BY KEY (col1,col2) PARTITIONS 8");
    }

    @Test
    public void testGeneratorRangeError() {
        assertGenerator("range_error.txt", "CREATE TABLE employees1 \n"
            + "(\n"
            + "   id         INT NOT NULL,\n"
            + "   first_name VARCHAR(30),\n"
            + "   last_name  VARCHAR(30),\n"
            + "   hired_date DATE NOT NULL,\n"
            + "   salary     INT NOT NULL,\n"
            + "   PRIMARY KEY(id,hired_date)\n"
            + ")\n"
            + "PARTITION BY RANGE (YEAR(hired_date))\n"
            + "(PARTITION p0 VALUES LESS THAN (1991),\n"
            + "PARTITION p1 VALUES LESS THAN (1996),\n"
            + "PARTITION p2 VALUES LESS THAN (2001),\n"
            + "PARTITION p3 VALUES LESS THAN (2006),\n"
            + "PARTITION p4 VALUES LESS THAN (2011),\n"
            + "PARTITION p5 VALUES LESS THAN (2016),\n"
            + "PARTITION p6 VALUES LESS THAN (2021),\n"
            + "PARTITION p7 VALUES LESS THAN MAXVALUE)");
    }

    @Test
    public void testTimestamp() {
        assertGenerator("simple10_timestamp.txt", "CREATE TABLE autotest_db_mysqlreader_1062102 \n"
            + "(\n"
            + "   field_timestamp TIMESTAMP DEFAULT NULL\n"
            + ")\n"
            + "DEFAULT CHARSET utf8mb4\n"
            + "ROW_FORMAT=DYNAMIC\n"
            + "COMPRESSION='zstd_1.3.8'\n"
            + "REPLICA_NUM=3\n"
            + "BLOCK_SIZE=16384\n"
            + "USE_BLOOM_FILTER=FALSE\n"
            + "TABLET_SIZE=134217728\n"
            + "PCTFREE=0\n"
            + "PROGRESSIVE_MERGE_NUM=10\n"
            + "MAX_USED_PART_ID=1");
    }

    private void assertGenerator(String file, String expect) {
        String text = getText(file);
        DialectNode dialectNode = new DialectNode(text);
        BaseStatement reverse = oceanBaseMysqlTransformer.reverse(dialectNode);
        Table table = oceanBaseMysqlTransformer.transformTable(reverse, OceanBaseContext.builder().build());
        assertText(expect, table);
    }

    @Test
    public void testGenerator() {
        List<Column> columns = Lists.newArrayList();
        Column e = Column.builder()
            .name("c1")
            .dataType("int")
            .comment("column_comment")
            .nullable(false)
            .build();
        columns.add(e);
        Table table = Table.builder()
            .name("t1")
            .comment("comment")
            .columns(columns)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1 \n"
            + "(\n"
            + "   c1 INT NOT NULL COMMENT 'column_comment'\n"
            + ")\n"
            + "COMMENT 'comment'", table);
    }

    @Test
    public void testPartitionRaw() {
        List<Column> columns = Lists.newArrayList();
        Column e = Column.builder()
            .name("c1")
            .dataType("int")
            .comment("column_comment")
            .nullable(false)
            .build();
        columns.add(e);
        List<BaseClientProperty> properties = Lists.newArrayList();
        StringProperty e1 = new StringProperty();
        e1.setKey(ExtensionPropertyKey.TABLE_PARTITION_RAW.getValue());
        e1.setValueString("PARTITION BY HASH(c1) PARTITIONS 8");
        properties.add(e1);
        Table table = Table.builder()
            .name("t1")
            .comment("comment")
            .columns(columns)
            .properties(properties)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1 \n"
            + "(\n"
            + "   c1 INT NOT NULL COMMENT 'column_comment'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITION BY HASH(c1) PARTITIONS 8", table);
    }

    private void assertText(String expect, Table table) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_OB_MYSQL)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        assertEquals(expect, generate.getDialectNodes().get(0).getNode());
    }
}
