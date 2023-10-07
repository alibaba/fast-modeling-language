package com.aliyun.fastmodel.transform.hologres.builder.v2;

import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/6/26
 */
public class QueryV2BuilderTest {

    QueryV2Builder queryBuilder = new QueryV2Builder();

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