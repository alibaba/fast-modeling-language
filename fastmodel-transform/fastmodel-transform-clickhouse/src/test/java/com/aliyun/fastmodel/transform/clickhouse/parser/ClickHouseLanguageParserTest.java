/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser;

import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/7/9
 */
public class ClickHouseLanguageParserTest {

    @Test
    public void parseNode() {
        ClickHouseLanguageParser clickHouseLanguageParser = new ClickHouseLanguageParser();
        CreateTable createTable = clickHouseLanguageParser.parseNode("CREATE TABLE dfv_v1 ( \n"
            + "    id String,\n"
            + "    c1 DEFAULT 1000,\n"
            + "    c2 String DEFAULT c1\n"
            + ") ENGINE = TinyLog;");
        int size = createTable.getColumnDefines().size();
        assertEquals(size, 3);
    }

    @Test
    public void testValue() {
        long l = Long.parseLong("+1");
        assertEquals(l, 1L);
    }
}