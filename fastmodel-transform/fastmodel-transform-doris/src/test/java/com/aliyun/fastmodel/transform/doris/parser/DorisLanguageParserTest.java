package com.aliyun.fastmodel.transform.doris.parser;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.doris.DorisTransformer;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * doris language parser test
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class DorisLanguageParserTest {

    DorisLanguageParser dorisLanguageParser = new DorisLanguageParser();

    @Test
    @SneakyThrows
    public void testParse() {
        String content = IOUtils.resourceToString("/doris/range.txt", Charset.defaultCharset());
        CreateTable o = dorisLanguageParser.parseNode(content);
        assertNotNull(o);
        List<ColumnDefinition> columnDefines = o.getColumnDefines();
        assertEquals(10, columnDefines.size());
    }

    @Test
    @SneakyThrows
    public void testParseRangeError() {
        String content = IOUtils.resourceToString("/doris/range_error.txt", Charset.defaultCharset());
        CreateTable o = dorisLanguageParser.parseNode(content);
        assertNotNull(o);
    }

    @Test
    public void testArrayIntParse() {
        CreateTable createTable = dorisLanguageParser.parseNode("create table a (array_int array<int> comment 'comment') comment 'abc';");
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        BaseDataType dataType = columnDefinition.getDataType();
        assertEquals("ARRAY<INT>", dataType.toString());

    }

    @Test
    public void testMapParse() {
        CreateTable createTable = dorisLanguageParser.parseNode("create table a (array_int map<string,string> comment 'comment') comment 'abc';");
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        ColumnDefinition columnDefinition = columnDefines.get(0);
        BaseDataType dataType = columnDefinition.getDataType();
        assertEquals("MAP<STRING,STRING>", dataType.toString());
    }

    @Test
    public void testParseStepPartition() {
        String sql = "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 INT MIN NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   FROM (\"2020-01-01\") TO (\"2023-01-01\") INTERVAL 1 DAY\n"
            + ");";
        CreateTable o = dorisLanguageParser.parseNode(sql);
        DorisTransformer dorisTransformer = new DorisTransformer();
        DialectNode transform = dorisTransformer.transform(o);
        assertEquals("CREATE TABLE abc\n"
            + "(\n"
            + "   c1 INT MIN NULL,\n"
            + "   c2 INT NOT NULL COMMENT \"comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PARTITION BY RANGE (c2)\n"
            + "(\n"
            + "   FROM (\"2020-01-01\") TO (\"2023-01-01\") INTERVAL 1 DAY\n"
            + ");", transform.getNode());
    }
}