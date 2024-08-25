package com.aliyun.fastmodel.transform.oceanbase;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * OceanBaseMysqlTransformerTest
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public class OceanBaseMysqlTransformerTest extends BaseOceanbaseTest {

    private OceanBaseMysqlTransformer oceanBaseMysqlTransformer = new OceanBaseMysqlTransformer();

    @Test
    public void transform() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition c = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("bigint", null))
            .build();
        columns.add(c);
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .comment(new Comment("table comment"))
            .build();
        DialectNode transform = oceanBaseMysqlTransformer.transform(createTable);
        assertEquals("CREATE TABLE abc \n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "COMMENT 'table comment'", transform.getNode());
    }

    @Test
    public void reverse() {
        String text = getText("simple1.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl1 \n"
            + "(\n"
            + "   c1 BIGINT UNSIGNED ZEROFILL NOT NULL,\n"
            + "   c2 VARCHAR(50),\n"
            + "   PRIMARY KEY(c1)\n"
            + ")", transform.getNode());
    }

    @Test
    public void testReverseIndex() {
        String text = getText("simple2_index.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl2 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT,\n"
            + "   c3 INT,\n"
            + "   INDEX i1 (c2)\n"
            + ")", transform.getNode());
    }

    @Test
    public void testReverseIndex2() {
        String text = getText("simple2_index2.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl3 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   UNIQUE KEY((c1 + c2)),\n"
            + "   INDEX i1 ((c1 + 1))\n"
            + ")", transform.getNode());
    }

    @Test
    public void testReverseHash() {
        String text = getText("simple3_hash.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl4 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT\n"
            + ")\n"
            + "PARTITION BY HASH (c1) PARTITIONS 8", transform.getNode());
    }

    @Test
    public void testReverseRange() {
        String text = getText("simple3_range.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl5 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   c3 INT\n"
            + ")\n"
            + "PARTITION BY RANGE (c1) SUBPARTITION BY KEY (c2,c3) SUBPARTITIONS 5\n"
            + "(PARTITION p0 VALUES LESS THAN (0),\n"
            + "PARTITION p1 VALUES LESS THAN (100))", transform.getNode());
    }

    @Test
    public void testReverseCharset() {
        String text = getText("simple4_charset.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl6 \n"
            + "(\n"
            + "   c1 VARCHAR(10),\n"
            + "   c2 VARCHAR(10) CHARSET GBK COLLATE gbk_bin\n"
            + ")\n"
            + "DEFAULT CHARSET utf8\n"
            + "COLLATE utf8mb4_general_ci", transform.getNode());
    }

    @Test
    public void testReverseCompress() {
        String text = getText("simple5_compress.txt");
        DialectNode transform = getDialectNode(text);
        assertEquals("CREATE TABLE tbl7 \n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 INT,\n"
            + "   c3 VARCHAR(64)\n"
            + ")\n"
            + "COMPRESSION='zstd_1.0'\n"
            + "ROW_FORMAT=DYNAMIC\n"
            + "PCTFREE=5", transform.getNode());
    }

    @Test
    public void testReversePr() {
        String text = getText("simple6_pr.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl8 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT\n"
            + ")\n"
            + "PARALLEL=3", node.getNode());
    }

    @Test
    public void testReverseAutoIncrement() {
        String text = getText("simple7_autoincrement.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl9 \n"
            + "(\n"
            + "   inv_id BIGINT NOT NULL AUTO_INCREMENT,\n"
            + "   c1     BIGINT,\n"
            + "   PRIMARY KEY(inv_id)\n"
            + ")\n"
            + "PARTITION BY HASH (inv_id) PARTITIONS 8", node.getNode());
    }

    @Test
    public void testReverseCheck() {
        String text = getText("simple8_check.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl10 \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 INT,\n"
            + "   col3 INT,\n"
            + "   CONSTRAINT equal_check1 CHECK(col1 = col3 * 2)\n"
            + ")", node.getNode());
    }

    @Test
    public void testReverseReference() {
        String text = getText("simple8_reference.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE ref_t2 \n"
            + "(\n"
            + "   c1 INT PRIMARY KEY,\n"
            + "   c2 INT,\n"
            + "   FOREIGN KEY(c2) REFERENCES ref_t1(c1) ON UPDATE SET NULL\n"
            + ")", node.getNode());
    }

    @Test
    public void testReversePtCombine1() {
        String text = getText("simple9_pt_combine_1.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tb1_m_rcr \n"
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
            + "PARTITION p2 VALUES LESS THAN (300))", node.getNode());
    }

    @Test
    public void testReversePtCombine2() {
        String text = getText("simple9_pt_combine_2.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl2_f_llc \n"
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
            + "))", node.getNode());
    }

    @Test
    public void testReversePtCombine3() {
        String text = getText("simple9_pt_combine_3.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl3_f_hk \n"
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
            + "))", node.getNode());
    }

    @Test
    public void testReversePtHash() {
        String text = getText("simple9_pt_hash.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl3_h \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 VARCHAR(50)\n"
            + ")\n"
            + "PARTITION BY HASH (col1) PARTITIONS 2", node.getNode());
    }

    @Test
    public void testReversePtList() {
        String text = getText("simple9_pt_list.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tbl2_l \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 DATE\n"
            + ")\n"
            + "PARTITION BY LIST (col1)\n"
            + "(PARTITION p0 VALUES IN (100),\n"
            + "PARTITION p1 VALUES IN (200))", node.getNode());
    }

    @Test
    public void testReversePtRange() {
        String text = getText("simple9_pt_range.txt");
        DialectNode node = getDialectNode(text);
        assertEquals("CREATE TABLE tb1_rc \n"
            + "(\n"
            + "   col1 INT,\n"
            + "   col2 INT\n"
            + ")\n"
            + "PARTITION BY RANGE COLUMNS (col1)\n"
            + "(PARTITION p0 VALUES LESS THAN (100),\n"
            + "PARTITION p1 VALUES LESS THAN (200),\n"
            + "PARTITION p2 VALUES LESS THAN (300))", node.getNode());
    }

    private DialectNode getDialectNode(String text) {
        DialectNode dialectNode = new DialectNode(text);
        BaseStatement reverse = oceanBaseMysqlTransformer.reverse(dialectNode);
        DialectNode transform = oceanBaseMysqlTransformer.transform(reverse);
        return transform;
    }

}