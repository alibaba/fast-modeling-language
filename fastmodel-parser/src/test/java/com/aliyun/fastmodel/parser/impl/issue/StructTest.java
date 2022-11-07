/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.parser.impl.issue;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * struct type test
 *
 * @author panguanjing
 * @date 2022/5/7
 */
public class StructTest extends BaseTest {

    @Test
    public void testStruct() {
        String code = "create dim table a (col ARRAY<STRUCT<`0`:STRING,`1`:STRING,`2`:STRING,`3`:STRING>>) comment 'abc';";
        CreateDimTable createDimTable = parse(code, CreateDimTable.class);
        assertEquals(createDimTable.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   col ARRAY<STRUCT<`0`:STRING,`1`:STRING,`2`:STRING,`3`:STRING>>\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testParse() {
        BaseDataType baseDataType = nodeParser.parseDataType(new DomainLanguage("ARRAY<STRUCT<`0`:STRING,`1`:STRING,`2`:STRING,`3`:STRING>>"));
        assertNotNull(baseDataType);
    }
}
