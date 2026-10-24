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

package com.aliyun.fastmodel.transform.mc;

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
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * @author panguanjing
 * @date 2020/11/18
 */
public class QueryTest {

    @Test
    public void testQueryJoinWithObject() {
        SelectItem selectItem = QueryUtil.aliasedName(new TableOrColumn(QualifiedName.of("dwd_fact_order.sku_code")),
            "abc");
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
        //junit.dwd_fct_order dwd_fct_order left join junit.dim_sku dim_sku (joinOn) left join junit.dim_shop
        // (joinOn) left join junit.dim_dept dim_dept (joinOn)
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
        assertNotNull(MaxComputeFormatter.formatSql(query, MaxComputeContext.builder().build()));
    }
}
