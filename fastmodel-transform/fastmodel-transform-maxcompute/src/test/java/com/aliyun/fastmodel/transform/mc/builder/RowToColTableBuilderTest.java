/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.builder;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.RowToCol;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * 行转列扽接口
 *
 * @author panguanjing
 * @date 2020/10/23
 */
public class RowToColTableBuilderTest {

    RowToColBuilder rowToColBuilder = new RowToColBuilder();

    @Test
    public void testRowToCol() {
        Query query = QueryUtil.simpleQuery(getSelectColStatement(), getFromStatement(), null, getGroupBy(), null, null,
            null, null);
        RowToCol rowToColStatement =
            new RowToCol(query);
        DialectNode build = rowToColBuilder.build(rowToColStatement, MaxComputeContext.builder().build());
        assertNotNull(build);
        assertNotNull(build.getNode());
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
            + "GROUP BY main.pk1,main.pk2,main.pk3\n";
        assertEquals(sql, build.getNode());

    }

    private GroupBy getGroupBy() {
        List<GroupingElement> groupingElements = ImmutableList.of(
            new SimpleGroupBy(
                ImmutableList.of(
                    new TableOrColumn(QualifiedName.of("t1.pk1")),
                    new TableOrColumn(QualifiedName.of("t1.pk2")),
                    new TableOrColumn(QualifiedName.of("t1.pk3"))
                )));
        return new GroupBy(false, groupingElements);
    }

    private BaseRelation getFromStatement() {
        Union relation = new Union(
            ImmutableList.of(
                new Union(
                    ImmutableList.of(getSelectStatement1(), getSelectStatement2()),
                    false
                ),
                getSelectStatement3()
            ),
            false
        );
        TableSubQuery tableSubQuery = new TableSubQuery(
            QueryUtil.query(relation)
        );
        Identifier main = new Identifier("main");
        return new AliasedRelation(tableSubQuery, main);

    }

    private BaseRelation getSelectStatement3() {
        return
            new QuerySpecification(
                QueryUtil.selectAll(
                    ImmutableList.of(
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "pk1")), "pk1"),
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "pk2")), "pk2"),
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "pk3")), "pk3"),
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "dim3")), "dim3"),
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "ind4")), "ind4"),
                        QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("t2", "ind5")), "ind5")
                    )),
                new Table(QualifiedName.of("t2"))
            );
    }

    private BaseRelation getSelectStatement2() {

        return
            new QuerySpecification(
                QueryUtil.selectAll(
                    ImmutableList.of(
                        QueryUtil.unAliasedName("pk1"),
                        QueryUtil.unAliasedName("pk2"),
                        QueryUtil.unAliasedName("pk3"),
                        QueryUtil.unAliasedName("dim_2"),
                        QueryUtil.unAliasedName("ind3")
                    )),
                new Table(QualifiedName.of("t2"))
            );
    }

    private BaseRelation getSelectStatement1() {
        return
            new QuerySpecification(
                QueryUtil.selectAll(
                    ImmutableList.of(
                        QueryUtil.unAliasedName("pk1"),
                        QueryUtil.unAliasedName("pk2"),
                        QueryUtil.unAliasedName("pk3"),
                        QueryUtil.unAliasedName("dim_1"),
                        QueryUtil.unAliasedName("ind1"),
                        QueryUtil.unAliasedName("ind2")
                    )),
                new Table(QualifiedName.of("t1"))
            );
    }

    private Select getSelectColStatement() {
        SelectItem pk1 = QueryUtil.unAliasedName("pk1");
        SelectItem pk2 = QueryUtil.unAliasedName("pk2");
        SelectItem pk3 = QueryUtil.unAliasedName("pk3");
        SelectItem dim1 = QueryUtil.unAliasedName("dim1");
        SelectItem dim2 = QueryUtil.unAliasedName("dim2");
        SelectItem dim3 = QueryUtil.unAliasedName("dim3");
        SelectItem ind1 = QueryUtil.unAliasedName("ind1");
        SelectItem ind2 = QueryUtil.unAliasedName("ind2");
        SelectItem ind3 = QueryUtil.unAliasedName("ind3");
        SelectItem ind4 = QueryUtil.unAliasedName("ind4");
        SelectItem ind5 = QueryUtil.unAliasedName("ind5");
        return QueryUtil.selectAll(ImmutableList.of(
            pk1, pk2, pk3, dim1, dim2, dim3, ind1, ind2, ind3, ind4, ind5
        ));

    }
}