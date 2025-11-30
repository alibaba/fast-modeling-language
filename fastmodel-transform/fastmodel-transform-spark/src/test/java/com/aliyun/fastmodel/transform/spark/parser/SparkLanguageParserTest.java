package com.aliyun.fastmodel.transform.spark.parser;

import com.aliyun.fastmodel.core.tree.Node;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/23
 */
public class SparkLanguageParserTest {

    @Test
    public void parseNode() {
        String parseNode = "create table a (a bigint) comment 'abc'";
        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        assertNotNull(o);
        assertEquals(o.toString(), "CREATE TABLE a \n"
            + "(\n"
            + "   a BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void parseNodeWithDecimal() {
        String parseNode = "create table a (a bigint, b decimal(10,2)) comment 'abc'";
        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        assertNotNull(o);
        assertEquals(o.toString(), "CREATE TABLE a \n"
            + "(\n"
            + "   a BIGINT,\n"
            + "   b DECIMAL(10,2)\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void parseNodeWithAllDataTypes() {
        // 测试所有Spark SQL支持的基本数据类型
        String parseNode = "CREATE TABLE test_table ("
            + "col_tinyint TINYINT, "
            + "col_smallint SMALLINT, "
            + "col_int INT, "
            + "col_bigint BIGINT, "
            + "col_float FLOAT, "
            + "col_double DOUBLE, "
            + "col_decimal DECIMAL(10,2), "
            + "col_string STRING, "
            + "col_binary BINARY, "
            + "col_boolean BOOLEAN, "
            + "col_date DATE, "
            + "col_timestamp TIMESTAMP"
            + ") COMMENT 'Test table with all data types'";

        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        assertNotNull(o);

        // 验证解析结果
        assertEquals(o.toString(), "CREATE TABLE test_table \n"
            + "(\n"
            + "   col_tinyint   TINYINT,\n"
            + "   col_smallint  SMALLINT,\n"
            + "   col_int       INT,\n"
            + "   col_bigint    BIGINT,\n"
            + "   col_float     FLOAT,\n"
            + "   col_double    DOUBLE,\n"
            + "   col_decimal   DECIMAL(10,2),\n"
            + "   col_string    STRING,\n"
            + "   col_binary    BINARY,\n"
            + "   col_boolean   BOOLEAN,\n"
            + "   col_date      DATE,\n"
            + "   col_timestamp TIMESTAMP\n"
            + ")\n"
            + "COMMENT 'Test table with all data types'");
    }

    @Test
    public void parseNodeWithComplexDataTypes() {
        // 测试复杂数据类型: ARRAY, MAP, STRUCT
        String parseNode = "CREATE TABLE test_complex_table ("
            + "col_array ARRAY<STRING>, "
            + "col_map MAP<STRING, INT>, "
            + "col_struct STRUCT<field1: STRING, field2: INT>"
            + ") COMMENT 'Test table with complex data types'";

        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        assertNotNull(o);

        // 验证解析结果
        assertEquals(o.toString(), "CREATE TABLE test_complex_table \n"
            + "(\n"
            + "   col_array  ARRAY<STRING>,\n"
            + "   col_map    MAP<STRING,INT>,\n"
            + "   col_struct STRUCT<field1:STRING,field2:INT>\n"
            + ")\n"
            + "COMMENT 'Test table with complex data types'");
    }
}