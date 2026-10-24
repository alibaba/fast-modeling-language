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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * 用于将转化处理
 *
 * @author panguanjing
 * @date 2020/10/21
 */
public class McEtlBuilderTransformTest {

    Transformer<BaseStatement> transformer = TransformerFactory.getInstance().get(DialectMeta.getMaxCompute());

    @Test
    public void buildSQL() {

        List<SelectItem> selectItems = new ArrayList<>();

        TableOrColumn e1 = new TableOrColumn(QualifiedName.of("example_table_1", "example_field_2"));
        TableOrColumn e2 = new TableOrColumn(QualifiedName.of("example_table_1", "example_field_3"));
        selectItems.add(
            QueryUtil.aliasedName(QueryUtil.functionCall("concat", e1, e2), "example_field_4"));

        TableOrColumn tableOrColumn = new TableOrColumn(QualifiedName.of("example_table_1", "example_field_5"));
        BaseExpression baseExpression = QueryUtil.functionCall("to_char", tableOrColumn, new StringLiteral("yyyyMMdd"));

        TableOrColumn tableOrColumn1 = new TableOrColumn(QualifiedName.of("example_table_1", "example_field_2"));
        TableOrColumn tableOrColumn2 = new TableOrColumn(QualifiedName.of("example_table_1", "example_field_3"));
        selectItems.add(QueryUtil.aliasedName(baseExpression, "example_field_5"));
        selectItems.add(QueryUtil.aliasedName(tableOrColumn1, "example_field_3"));
        selectItems.add(QueryUtil.aliasedName(tableOrColumn2, "example_field_2"));

        BaseExpression baseExpression1 = QueryUtil.functionCall("max",
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_6")));
        selectItems.add(QueryUtil.aliasedName(baseExpression1, "example_field_6"));

        BaseExpression baseExpression2 = QueryUtil.functionCall("max",
            new TableOrColumn(QualifiedName.of("example_field_7", "example_field_8")));

        selectItems.add(QueryUtil.aliasedName(baseExpression2, "example_field_8"));

        BaseExpression baseExpression3 = QueryUtil.functionCall("sum",
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_9")));
        selectItems.add(QueryUtil.aliasedName(baseExpression3, "example_field_10"));

        BaseExpression baseExpression4 = QueryUtil.functionCall("count", new LongLiteral("1"));
        selectItems.add(QueryUtil.aliasedName(baseExpression4, "example_field_11"));

        Select select = new Select(selectItems, false);

        ComparisonExpression expression = new ComparisonExpression(ComparisonOperator.EQUAL,
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_2")),
            new TableOrColumn(QualifiedName.of("example_table_12", "example_field_2")));

        ComparisonExpression expression1 = new ComparisonExpression(ComparisonOperator.EQUAL,
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_3")),
            new TableOrColumn(QualifiedName.of("example_table_12", "example_field_3")));

        LogicalBinaryExpression logicalBinaryExpression1 = new LogicalBinaryExpression(
            LogicalOperator.AND,
            expression, expression1
        );
        Join join = new Join(JoinToken.LEFT,
            new Table(QualifiedName.of("example_table_13")),
            new Join(
                JoinToken.LEFT,
                new Table(QualifiedName.of("example_table_1")),
                new Table(QualifiedName.of("example_table_12")),
                new JoinOn(expression)
            ), new JoinOn(logicalBinaryExpression1)
        );
        //where
        StringLiteral stringLiteral = new StringLiteral("example_kind_a");
        ComparisonExpression build1 = ComparisonExpression.builder().operator(ComparisonOperator.EQUAL).left(
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_15")))
            .right(stringLiteral).build();
        ComparisonExpression build2 = ComparisonExpression.builder().operator(ComparisonOperator.EQUAL).left(
            new TableOrColumn(QualifiedName.of("example_table_1", "example_field_16"))).
            right(new StringLiteral("example_group_a")).build();
        ComparisonExpression build3 = ComparisonExpression.builder().operator(ComparisonOperator.EQUAL).left(
            new TableOrColumn(QualifiedName.of("example_field_7", "example_field_17"))).
            right(new StringLiteral("example_group_b")).build();

        LogicalBinaryExpression logicalBinaryExpression =
            new LogicalBinaryExpression(LogicalOperator.AND,
                build1, new LogicalBinaryExpression(LogicalOperator.AND, build2, build3));

        //groupby
        FunctionCall functionCall = QueryUtil.functionCall("to_char", new TableOrColumn(QualifiedName.of("example_table_1", "example_field_5")),
            new StringLiteral("yyyyMMdd"));
        SimpleGroupBy groupBy = new SimpleGroupBy(
            ImmutableList.of(new TableOrColumn(QualifiedName.of("example_table_1", "example_field_2")), functionCall));
        GroupBy list = new GroupBy(false, ImmutableList.of(groupBy));

        QuerySpecification querySpecification = QuerySpecification.builder().where(logicalBinaryExpression).select(
            select).from(join).groupBy(list).build();

        //build SQL
        Query selectStatement = QueryUtil.query(querySpecification);

        DialectNode dialectNode = transformer.transform(selectStatement, MaxComputeContext.builder().build());
        assertNotNull(dialectNode.getNode());
        String result
            = " SELECT concat(example_table_1.example_field_2, example_table_1.example_field_3) example_field_4 , to_char(example_table_1.example_field_5, 'yyyyMMdd') example_field_5 , example_table_1.example_field_2 "
            + "example_field_3 , example_table_1.example_field_3 example_field_2 , max(example_table_1.example_field_6) example_field_6 , max(example_field_7.example_field_8) example_field_8 , sum(example_table_1"
            + ".example_field_9) example_field_10 , count(1) example_field_11 FROM example_table_13 LEFT JOIN example_table_1 LEFT JOIN example_table_12 ON example_table_1.example_field_2 "
            + "= example_table_12.example_field_2 ON example_table_1.example_field_2 = example_table_12.example_field_2 AND example_table_1.example_field_3 = example_table_12.example_field_3 WHERE example_table_1.example_field_15 = "
            + "'example_kind_a' AND example_table_1.example_field_16 = 'example_group_a' AND example_field_7.example_field_17 = 'example_group_b' GROUP BY example_table_1.example_field_2,to_char(example_table_1.example_field_5, "
            + "'yyyyMMdd')";
        String target = dialectNode.getNode();
        MatcherAssert.assertThat(target, Matchers.equalToIgnoringWhiteSpace(result));
    }

}
