/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.parser;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.MaxComputeTransformer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/18
 */
public class MaxComputeQueryTest {
    MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();

    @Test
    public void testQueryTest() {
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode("select * from abc;"));
        assertEquals("SELECT *\n"
            + "FROM\n"
            + "  abc\n", reverse.toString());
    }

    @Test
    public void testSubQuery() {
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode("SELECT  a\n"
            + "        ,MAX(b)\n"
            + "FROM    (\n"
            + "            SELECT  a\n"
            + "                    ,b\n"
            + "            FROM    (\n"
            + "                        SELECT  *\n"
            + "                        FROM    public.test\n"
            + "                    ) \n"
            + "        ) \n"
            + "WHERE   b = '10'"));
        Query query = (Query)reverse;
        assertNotNull(query);
    }
}
