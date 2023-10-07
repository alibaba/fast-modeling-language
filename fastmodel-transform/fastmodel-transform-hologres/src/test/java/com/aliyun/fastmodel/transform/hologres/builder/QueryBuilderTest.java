/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.builder;

import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/20
 */
public class QueryBuilderTest {

    QueryBuilder queryBuilder = new QueryBuilder();

    @Test
    public void testAlias() {
        Query query = FastModelParserFactory.getInstance().get().parseStatement("SELECT  a\n"
            + "        ,MAX(b)\n"
            + "FROM    (\n"
            + "            SELECT  a\n"
            + "                    ,b\n"
            + "            FROM    (\n"
            + "                        SELECT  *\n"
            + "                        FROM    public.test\n"
            + "                    ) \n"
            + "        ) \n"
            + "WHERE   b = '10'");
        DialectNode build = queryBuilder.build(query, HologresTransformContext.builder().build());
        assertEquals(build.getNode(), "SELECT\n"
            + "  sub2.a\n"
            + ", MAX(sub2.b)\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        sub1.a\n"
            + "      , sub1.b\n"
            + "      FROM\n"
            + "        (\n"
            + "            SELECT *\n"
            + "            FROM\n"
            + "              test\n"
            + "         )  sub1\n"
            + "   )  sub2\n"
            + "WHERE sub2.b = '10'\n");
    }

}