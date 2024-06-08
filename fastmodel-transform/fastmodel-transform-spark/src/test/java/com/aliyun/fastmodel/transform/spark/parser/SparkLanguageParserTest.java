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
}