/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.format;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.transform.api.context.setting.QuerySetting;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresRewriteVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Query process
 *
 * @author panguanjing
 * @date 2022/6/24
 */
public class HologresRewriteVisitorTest {

    FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();

    @Test
    public void visitQuery() {
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        Query query = fastModelParser.parseStatement("SELECT  *\n"
            + "FROM    project.test\n"
            + ";");
        Query node = (Query)hologresRewriteVisitor.visitQuery(query, null);
        QuerySpecification querySpecification = (QuerySpecification)node.getQueryBody();
        Table table = (Table)querySpecification.getFrom();
        assertEquals(table.getName(), QualifiedName.of("test"));
    }

    @Test
    public void visitQueryKeepSchema() {
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor(
            HologresTransformContext.builder()
                .querySetting(QuerySetting.builder().keepSchemaName(true).build())
                .build()
        );
        Query query = fastModelParser.parseStatement("SELECT  *\n"
            + "FROM    project.test\n"
            + ";");
        Query node = (Query)hologresRewriteVisitor.visitQuery(query, null);
        QuerySpecification querySpecification = (QuerySpecification)node.getQueryBody();
        Table table = (Table)querySpecification.getFrom();
        assertEquals(table.getName(), QualifiedName.of("project.test"));
    }

    @Test
    public void visitQueryAlias() {
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        Query query = fastModelParser.parseStatement("SELECT  a\n"
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
        String result = rewrite(hologresRewriteVisitor, query);
        assertEquals(result, "SELECT\n"
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
            + "WHERE sub2.b = '10'\n"
            + ";");
    }

    @Test
    public void testCountRewrite() {
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        Query query = fastModelParser.parseStatement("SELECT  Max(a)\n"
            + "        ,COUNT(DISTINCT id) AS count_number\n"
            + "FROM    (\n"
            + "            SELECT  id\n"
            + "                    ,a\n"
            + "                    ,b\n"
            + "            FROM    public.test\n"
            + "        ) \n"
            + "GROUP BY b\n"
            + "ORDER BY count_number\n"
            + ";");
        String result = rewrite(hologresRewriteVisitor, query);
        assertEquals(result, "SELECT\n"
            + "  Max(sub1.a)\n"
            + ", COUNT(DISTINCT sub1.id) AS count_number\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        id\n"
            + "      , a\n"
            + "      , b\n"
            + "      FROM\n"
            + "        test\n"
            + "   )  sub1\n"
            + "GROUP BY sub1.b\n"
            + "ORDER BY count_number\n"
            + ";");
    }

    private String rewrite(HologresRewriteVisitor hologresRewriteVisitor, Query query) {
        Query node = (Query)hologresRewriteVisitor.visitQuery(query, null);
        return HologresFormatter.format(node, HologresTransformContext.builder().appendSemicolon(true).build()).getNode();
    }

    @Test
    public void testSubJoin() {
        Query query = fastModelParser.parseStatement("SELECT\n"
            + "    *\n"
            + "FROM\n"
            + "    public.table3 c\n"
            + "    JOIN (public.table1\n"
            + "        JOIN public.table2 b ON name = b.name) ON (b.name = c.name);\n");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT sub1.*\n"
            + "FROM\n"
            + "  table3 c\n"
            + " JOIN table1 sub1\n"
            + " JOIN table2 b ON sub1.name = b.name ON b.name = c.name\n"
            + ";");
    }

    @Test
    public void testAliasSubQuery() {
        assertTransform("SELECT  a\n"
            + "        ,MAX(b)\n"
            + "FROM    (\n"
            + "            SELECT  a\n"
            + "                    ,b\n"
            + "            FROM    (\n"
            + "                        SELECT  *\n"
            + "                        FROM    public.test\n"
            + "                    ) \n"
            + "        ) \n"
            + "WHERE   b = '10'", "SELECT\n"
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
            + "WHERE sub2.b = '10'\n"
            + ";");
    }

    @Test
    public void testSelectItem() {
        Query query = fastModelParser.parseStatement("select a.b from a;");
        QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
        assertNotNull(querySpecification);
        query = fastModelParser.parseStatement("select a.* from a;");
        assertNotNull(query);
        query = fastModelParser.parseStatement("select * from a where b = '10';");
        assertNotNull(query);
        query = fastModelParser.parseStatement("select max(b) from a;");
        assertNotNull(query);
    }

    @Test
    public void testJoin() {
        Query query = fastModelParser.parseStatement("SELECT\n"
            + "    *\n"
            + "FROM\n"
            + "    table3 c\n"
            + "    JOIN (table1 sub1\n"
            + "        JOIN table2 b ON (sub1.name = b.name)) ON (b.name = c.name);\n");
        assertNotNull(query);
    }

    @Test
    public void testAlias() {
        Query query = fastModelParser.parseStatement("select a as alias_1 from public.test;");
        assertNotNull(query);
    }

    @Test
    public void testAliasRewrite() {
        Query query = fastModelParser.parseStatement("SELECT  a AS alias_1\n"
            + "        ,b AS `alias_2`\n"
            + "FROM    public.test\n"
            + ";");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT\n"
            + "  a AS alias_1\n"
            + ", b AS \"alias_2\"\n"
            + "FROM\n"
            + "  test\n;");
    }

    @Test
    public void testRowNumber() {
        Query query = fastModelParser.parseStatement("SELECT  ROW_NUMBER() OVER (PARTITION BY a ORDER BY a)\n"
            + "FROM    public.test\n"
            + ";");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT ROW_NUMBER() OVER (ORDER BY a)\n"
            + "FROM\n"
            + "  test\n"
            + ";");
    }

    @Test
    public void testDatePart() {
        Query query = fastModelParser.parseStatement("SELECT  DATEPART(timestamp, 'hh')\n"
            + "FROM    public.test\n"
            + ";\n");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT to_char(timestamp, 'hh')\n"
            + "FROM\n"
            + "  test\n;");
    }

    @Test
    public void testFromUnix() {
        Query query = fastModelParser.parseStatement("SELECT  FROM_UNIXTIME(time / 1000)\n"
            + "FROM    public.test\n"
            + ";");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT to_timestamp(time / 1000)\n"
            + "FROM\n"
            + "  test\n"
            + ";");
    }

    @Test
    public void testMaxPt() {
        assertTransform("SELECT  *\n"
            + "FROM    test\n"
            + "WHERE   ds = MAX_PT('project.table1')\n"
            + ";", "SELECT *\n"
            + "FROM\n"
            + "  test\n"
            + "WHERE ds = MAX_PT('table1')\n"
            + ";");
    }

    @Test
    public void testDistributeBy() {
        assertTransform("SELECT  b\n"
            + "FROM    public.test\n"
            + "DISTRIBUTE BY b\n"
            + "SORT BY b\n"
            + ";\n", "SELECT b\n"
            + "FROM\n"
            + "  test\n"
            + "ORDER BY b\n"
            + ";");
    }

    @Test
    public void testAliasColumn() {
        Query query = fastModelParser.parseStatement("SELECT  student_no AS student_no\n"
            + "        ,student_name AS student_name\n"
            + "        ,gender AS gender\n"
            + "        ,age AS age\n"
            + "        ,address AS address\n"
            + "        ,tel_no AS tel_no\n"
            + "        ,gmt_term AS gmt_term\n"
            + "FROM    streamstudiotest.dim_student\n"
            + "WHERE   student_name = ${student_name}");
        HologresRewriteVisitor hologresRewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(hologresRewriteVisitor, query);
        assertEquals(rewrite, "SELECT\n"
            + "  student_no AS student_no\n"
            + ", student_name AS student_name\n"
            + ", gender AS gender\n"
            + ", age AS age\n"
            + ", address AS address\n"
            + ", tel_no AS tel_no\n"
            + ", gmt_term AS gmt_term\n"
            + "FROM\n"
            + "  dim_student\n"
            + "WHERE student_name = ${student_name}\n"
            + ";");
    }

    @Test
    public void testAliasIdentifier() {
        assertTransform("SELECT  a AS alias_1\n"
            + "        ,b AS `alias_2`\n"
            + "FROM    public.test\n"
            + ";", "SELECT\n"
            + "  a AS alias_1\n"
            + ", b AS \"alias_2\"\n"
            + "FROM\n"
            + "  test\n"
            + ";");
    }

    @Test
    public void testJoinAlias() {
        assertTransform("SELECT  COUNT(DISTINCT id) AS comment_cnt\n"
            + "        ,max(nick_name) AS nick_name\n"
            + "FROM    schema1.table1 \n"
            + "LEFT JOIN (\n"
            + "              SELECT  *\n"
            + "              FROM    schema2.table2\n"
            + "              WHERE   ds = MAX_PT('schema2.table2')\n"
            + "          ) worker\n"
            + "ON      author_staff_id = worker.work_no\n"
            + "WHERE   (\n"
            + "                worker.super_work_no IN ('017300','042129','064460')\n"
            + "            OR  author_staff_id IN ('017300','042129','064460')\n"
            + "        )\n"
            + "AND     author_staff_id NOT LIKE \"WB%\"\n"
            + "AND     created_at BETWEEN \"${START}\"\n"
            + "AND     '2022-05-04 23:59:59'\n"
            + "GROUP BY author_staff_id\n"
            + "ORDER BY comment_cnt DESC\n"
            + ";", "SELECT\n"
            + "  COUNT(DISTINCT sub2.id) AS comment_cnt\n"
            + ", max(sub2.nick_name) AS nick_name\n"
            + "FROM\n"
            + "  table1 sub1\n"
            + "LEFT JOIN ( (\n"
            + "            SELECT sub1.*\n"
            + "            FROM\n"
            + "              table2\n"
            + "            WHERE sub1.ds = MAX_PT('table2')\n"
            + "         )  sub2   ) worker ON sub1.author_staff_id = worker.work_no\n"
            + "WHERE (worker.super_work_no IN ('017300', '042129', '064460') OR author_staff_id IN ('017300', '042129', '064460')) AND NOT "
            + "(author_staff_id LIKE 'WB%') AND created_at BETWEEN '${START}' AND '2022-05-04 23:59:59'\n"
            + "GROUP BY sub2.author_staff_id\n"
            + "ORDER BY comment_cnt DESC\n"
            + ";");
    }

    @Test
    public void testAliasJudge() {

        assertTransform("SELECT  Max(a)\n"
            + "        ,COUNT(DISTINCT id) AS count_number\n"
            + "FROM    (\n"
            + "            SELECT  id\n"
            + "                    ,a\n"
            + "                    ,b\n"
            + "            FROM    public.test\n"
            + "        ) \n"
            + "GROUP BY b\n"
            + "ORDER BY count_number\n"
            + ";\n", "SELECT\n"
            + "  Max(sub1.a)\n"
            + ", COUNT(DISTINCT sub1.id) AS count_number\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        id\n"
            + "      , a\n"
            + "      , b\n"
            + "      FROM\n"
            + "        test\n"
            + "   )  sub1\n"
            + "GROUP BY sub1.b\n"
            + "ORDER BY count_number\n"
            + ";");
    }

    @Test
    public void testAssertUpper() {
        assertTransform("select * from abc where id = ${ID};", "SELECT *\n"
            + "FROM\n"
            + "  abc\n"
            + "WHERE id = ${ID}\n"
            + ";");
    }

    @Test
    public void testAssertUpper2() {
        assertTransform("select * from abc where id = `${ID}`;", "SELECT *\n"
            + "FROM\n"
            + "  abc\n"
            + "WHERE id = \"${ID}\"\n"
            + ";");
    }

    private void assertTransform(String input, String expect) {
        Query query = fastModelParser.parseStatement(input);
        HologresRewriteVisitor rewriteVisitor = new HologresRewriteVisitor();
        String rewrite = rewrite(rewriteVisitor, query);
        assertEquals(expect, rewrite);
    }

}