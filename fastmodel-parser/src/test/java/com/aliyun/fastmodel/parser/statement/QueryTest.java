/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.parser.statement;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.select.Hint;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.parser.NodeParser;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author panguanjing
 * @date 2020/11/18
 */
public class QueryTest {

    private final NodeParser nodeParser = new NodeParser();

    @Test
    public void testUnion() {
        String sql = "SELECT\n"
            + "  main.pk1 pk1\n"
            + ", main.pk2 pk2\n"
            + ", main.pk3 pk3\n"
            + ", max(main.dim1) dim1\n"
            + ", max(main.dim2) dim2\n"
            + ", max(main.dim3) dim3\n"
            + ", max(main.ind1) ind1\n"
            + ", max(main.ind2) ind2\n"
            + ", max(main.ind3) ind3\n"
            + ", max(main.ind4) ind4\n"
            + ", max(main.ind5) ind5\n"
            + "FROM\n"
            + "  (\n"
            + "      SELECT\n"
            + "        pk1\n"
            + "      , pk2\n"
            + "      , pk3\n"
            + "      , NULL dim1\n"
            + "      , NULL dim2\n"
            + "      , NULL dim3\n"
            + "      , ind1\n"
            + "      , ind2\n"
            + "      , NULL ind3\n"
            + "      , NULL ind4\n"
            + "      , NULL ind5\n"
            + "      FROM\n"
            + "        t1\n"
            + "UNION ALL       SELECT\n"
            + "        pk1\n"
            + "      , pk2\n"
            + "      , pk3\n"
            + "      , NULL dim1\n"
            + "      , NULL dim2\n"
            + "      , NULL dim3\n"
            + "      , NULL ind1\n"
            + "      , NULL ind2\n"
            + "      , ind3\n"
            + "      , NULL ind4\n"
            + "      , NULL ind5\n"
            + "      FROM\n"
            + "        t2\n"
            + "UNION ALL       SELECT\n"
            + "        t2.pk1 pk1\n"
            + "      , t2.pk2 pk2\n"
            + "      , t2.pk3 pk3\n"
            + "      , NULL dim1\n"
            + "      , NULL dim2\n"
            + "      , t2.dim3 dim3\n"
            + "      , NULL ind1\n"
            + "      , NULL ind2\n"
            + "      , NULL ind3\n"
            + "      , t2.ind4 ind4\n"
            + "      , t2.ind5 ind5\n"
            + "      FROM\n"
            + "        t2\n"
            + "   )  main\n"
            + "GROUP BY main.pk1, main.pk2, main.pk3\n";

        Query parse = (Query)nodeParser.parse(new DomainLanguage(sql));
        assertEquals(parse.getQueryBody().getClass(), QuerySpecification.class);
        QuerySpecification specification = (QuerySpecification)parse.getQueryBody();
        BaseRelation from = specification.getFrom();
        assertEquals(from.getClass(), AliasedRelation.class);

        String s = FastModelFormatter.formatNode(parse);
        assertThat(s, Matchers.equalTo(sql));

    }

    @Test
    public void testQueryJoin() {
        String sql = "SELECT '${bizdate}' AS ds\n"
            + "\t, concat(dwd_fct_order.sku_code, '-', dwd_fct_order.shop_code, '-', dim_sku.dept_code) AS sys_dim_pk\n"
            + "\t, dwd_fct_order.sku_code AS sku_code, dwd_fct_order.shop_code AS shop_code, dim_sku.dept_code AS "
            + "dept_code\n"
            + "\t, sum(CASE \n"
            + "\t\tWHEN dim_dept.type = '1' THEN dwd_fct_order.price\n"
            + "\tEND) AS sku_amount\n"
            + "\t, sum(CASE \n"
            + "\t\tWHEN dim_shop.type = '2' THEN dwd_fct_order.item_cnt\n"
            + "\tEND) / sum(CASE \n"
            + "\t\tWHEN dim_shop.type = '2' THEN dwd_fct_order.price\n"
            + "\tEND) AS sku_price_avg\n"
            + "FROM junit.dwd_fct_order dwd_fct_order\n"
            + "\tLEFT JOIN junit.dim_sku dim_sku\n"
            + "\tON dwd_fct_order.sku_code = dim_sku.sku_code\n"
            + "\t\tAND dwd_fct_order.shop_code = dim_sku.shop_code\n"
            + "\tLEFT JOIN junit.dim_shop dim_shop ON dwd_fct_order.shop_code = dim_shop.shop_code\n"
            + "\tLEFT JOIN junit.dim_dept dim_dept ON dim_sku.dept_code = dim_dept.dept_code\n"
            + "WHERE dwd_fct_order.ds = '${bizate}'\n"
            + "\tAND dim_sku.ds = '${bizate}'\n"
            + "\tAND dim_shop.ds = '${bizate}'\n"
            + "\tAND dim_dept.ds = '${bizate}'\n"
            + "\tAND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') >= to_date('${bizdate}', 'yyyymmdd')\n"
            + "\tAND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') < dateadd(to_date('${bizdate}', 'yyyymmdd'), 1, 'dd')\n"
            + "GROUP BY dwd_fct_order.sku_code, dwd_fct_order.shop_code, dim_sku.dept_code";

        Query parse = (Query)nodeParser.parse(new DomainLanguage(sql));
        System.out.println(parse);
    }

    @Test
    public void testQueryJoinWithObject() {
        SelectItem selectItem = QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("dwd_fact_order.sku_code")),
            "");
        ComparisonExpression expression = new ComparisonExpression(
            ComparisonOperator.EQUAL,
            new TableOrColumn(QualifiedName.of("dim_sku", "dept_code")),
            new TableOrColumn(QualifiedName.of("dim_dept.dept_code")));

        ComparisonExpression expression2 = new ComparisonExpression(
            ComparisonOperator.EQUAL,
            new TableOrColumn(QualifiedName.of("dwd_fct_order.shop_code")),
            new TableOrColumn(QualifiedName.of("dim_shop.shop_code"))
        );
        BaseExpression expression3 = new ComparisonExpression(
            ComparisonOperator.EQUAL,
            new TableOrColumn(QualifiedName.of("dwd_fct_order.sku_code")),
            new TableOrColumn(QualifiedName.of("dim_sku.sku_code"))
        );

        Join join = new Join(
            JoinToken.LEFT,
            new Join(
                JoinToken.LEFT,
                new Join(
                    JoinToken.LEFT,
                    new AliasedRelation(new Table(QualifiedName.of("junit.dwd_fact_order")),
                        new Identifier("dwd_fct_order")),
                    new AliasedRelation(new Table(QualifiedName.of("junit.dim_sku")), new Identifier("dim_sku")),
                    new JoinOn(expression3)
                ),
                new AliasedRelation(new Table(QualifiedName.of("junit.dim_shop")), new Identifier("dim_shop")),
                new JoinOn(expression2)
            ),
            new AliasedRelation(
                new Table(QualifiedName.of("junit.dim_dept")), new Identifier("dim_dept")
            ),
            new JoinOn(expression)

        );
        Query query = QueryUtil.simpleQuery(
            QueryUtil.selectAll(ImmutableList.of(selectItem)),
            join,
            null
        );
        assertNotNull(query);
    }

    @Test
    public void testWith() {
        String sql = "with a (id) as (with x as (select 123 from z) select * from x) " +
            "   , b (id) as (select 999 from z) " +
            "select * from a join b using (id) order by a offset 1 limit 10";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(sql));
        Query query = (Query)parse;
        assertNotNull(query.getWith());
    }

    @Test
    public void testJoinWithRight() {
        String sql = "SELECT A FROM B RIGHT JOIN C USING(ID)";
        Query query = (Query)nodeParser.parse(new DomainLanguage(sql));
        QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
        Join join = (Join)querySpecification.getFrom();
        assertEquals(join.getJoinToken(), JoinToken.RIGHT);
    }

    @Test
    public void testHint() {
        String sql = "select /*+ COALESCE(1), REPARTITION(3) */ a from b;";
        Query query = nodeParser.parseStatement(sql);
        QuerySpecification queryBody = (QuerySpecification)query.getQueryBody();
        List<Hint> hints = queryBody.getHints();
        assertEquals(2, hints.size());
    }

    @Test
    public void testHintKeyValue() {
        String sql = "select /*+ engineName(maxcompute) */ a from b; ";
        Query query = nodeParser.parseStatement(sql);
        QuerySpecification specification = (QuerySpecification)query.getQueryBody();
        List<Hint> hints = specification.getHints();
        for (Hint hint : hints) {
            Identifier hintName = hint.getHintName();
            assertNotNull(hintName);
        }

    }

    @Test
    public void testReverse() {
        String sql = "-- this sql is logic sql, and ignores the partition\n"
            + "select\n"
            + "  -- generate by dimension\n"
            + "    concat(dwd_fct_order.sku_code, '#', dwd_fct_order.shop_code, '#', dim_sku.dept_code) as dim_pk\n"
            + "  , dwd_fct_order.sku_code as sku_code\n"
            + "  , dwd_fct_order.shop_code as shop_code\n"
            + "  , dim_sku.dept_code as dept_code\n"
            + "\n"
            + "  -- generate by derivative's caculate expression\n"
            + "  , count(1) as pay_count\n"
            + "\n"
            + " -- generate by dimension model\n"
            + " from dwd_fct_order as dwd_fct_order\n"
            + " left join dim_sku as dim_sku\n"
            + "  on dwd_fct_order.sku_code = dim_sku.sku_code\n"
            + "  and dwd_fct_order.shop_code = dim_sku.shop_code\n"
            + " left join dim_shop as dim_shop\n"
            + "  on dwd_fct_order.shop_code = dim_shop.shop_code\n"
            + " left join dim_dept as dim_dept\n"
            + "  on dim_sku.dept_code = dim_dept.dept_code\n"
            + "\n"
            + "where  1=1 \n"
            + "\n"
            + " -- generate by time period\n"
            + " and to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') >= TO_BEGIN_DATE_WITH_FIRST_DAY('${bizdate}', 'w', -2,"
            + " 1)\n"
            + " and to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') < TO_END_DATE_WITH_FIRST_DAY('${bizdate}', 'w', -1, 1)\n"
            + "\n"
            + " -- generate by adjunct\n"
            + " and dim_sku.sku_type = '1'\n"
            + " and dim_shop.shop_type = '2'\n"
            + " and dim_dept.dept_type = '3'\n"
            + "\n"
            + "-- generate by dimension\n"
            + "group by dwd_fct_order.sku_code, dwd_fct_order.shop_code, dim_sku.dept_code";

        Query query = nodeParser.parseStatement(sql);
        assertNotNull(query);

    }

    @Test
    public void testJoin() {
        String query
            = "select * from junit.dwd_fact_order dwd_fct_order "
            + "LEFT JOIN t0 a0 ON t0.f0 = r0.f0 "
            + "LEFT JOIN t1 a1 ON t1.f1 = r1.f1 "
            + "LEFT JOIN t2 a2 ON t2.f2 = r2.f2 "
            + "LEFT JOIN t3 a3 ON t3.f3 = r3.f3 "
            + "LEFT JOIN t4 a4 ON t4.f4 = r4.f4";

        BaseStatement statement = nodeParser.parseStatement(query);
        String result = FastModelFormatter.formatNode(statement);
        assertEquals("SELECT *\n"
            + "FROM\n"
            + "  junit.dwd_fact_order dwd_fct_order\n"
            + "LEFT JOIN t0 a0 ON t0.f0 = r0.f0\n"
            + "LEFT JOIN t1 a1 ON t1.f1 = r1.f1\n"
            + "LEFT JOIN t2 a2 ON t2.f2 = r2.f2\n"
            + "LEFT JOIN t3 a3 ON t3.f3 = r3.f3\n"
            + "LEFT JOIN t4 a4 ON t4.f4 = r4.f4\n", result);
    }

    @Test
    public void testWithCTE() {
        String query = "with t1 as (select a, max(b) as b from x group by a) select t1.* from t1";
        BaseStatement statement = nodeParser.parseStatement(query);
        String r = FastModelFormatter.formatNode(statement);
        assertEquals("WITH\n"
            + "  t1 AS (\n"
            + "   SELECT\n"
            + "     a\n"
            + "   , max(b) AS b\n"
            + "   FROM\n"
            + "     x\n"
            + "   GROUP BY a\n"
            + ") \n"
            + "SELECT t1.*\n"
            + "FROM\n"
            + "  t1\n", r);
    }

    @Test
    public void testWithCube() {
        String query = "SELECT origin_state, origin_zip, destination_state, sum(package_weight) FROM "
            + "shipping GROUP BY CUBE(origin_state, origin_zip)";
        BaseStatement statement = nodeParser.parseStatement(query);
        String result = FastModelFormatter.formatNode(statement);
        assertEquals("SELECT\n"
            + "  origin_state\n"
            + ", origin_zip\n"
            + ", destination_state\n"
            + ", sum(package_weight)\n"
            + "FROM\n"
            + "  shipping\n"
            + "GROUP BY CUBE (origin_state, origin_zip)\n", result);

    }

    @Test
    public void testQueryWithoutFrom() {
        String query = "select dim_shop.xx, dim_shop.shop_name, \n"
            + " ind_a, ind_b,ind_c  where xxx order by xxx";
        BaseStatement statement = nodeParser.parseStatement(query);
        assertEquals(statement.getClass(), Query.class);
    }

    @Test
    public void testClusterBy() {
        String query = "SELECT  b\n"
            + "FROM    public.test\n"
            + "DISTRIBUTE BY b\n"
            + "SORT BY b\n"
            + ";";
        BaseStatement baseStatement = nodeParser.parseStatement(query);
        Query query1 = (Query)baseStatement;
        QuerySpecification queryBody = (QuerySpecification)query1.getQueryBody();
        assertNotNull(queryBody.getDistributeBy());
        assertNotNull(queryBody.getSortBy());
    }
}
