/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.sqlconvert;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.sqlconvert.SqlConverter;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.mc.builder.rules.BaseRulesBuilder;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/7
 */
public class UniqueKeyConverter implements SqlConverter<TableFunction, MaxComputeContext> {
    /**
     * 唯一健的转换
     *
     * @param tableName        表名
     * @param baseFunction     函数
     * @param matchExpression  表达式
     * @param transformContext
     * @return
     */
    @Override
    public String convert(QualifiedName tableName, TableFunction baseFunction, String matchExpression, MaxComputeContext transformContext) {
        /**
         * select count(*) from (select student_name,teacher_name from bbbb where ds='$[yyyymmdd]' group by
         * student_name,teacher_name having count(*) > 1) t;
         */
        ComparisonExpression count = new ComparisonExpression(ComparisonOperator.GREATER_THAN,
            new FunctionCall(QualifiedName.of("count"), false, ImmutableList.of()), new LongLiteral("1"));

        //support mc 3layer
        QualifiedName fromTable = StringJoinUtil.join(transformContext.getMcProject(), transformContext.getMcSchema(), tableName.getSuffix());
        CustomExpression dqc = new CustomExpression("$dqc");
        boolean usePartition = StringUtils.isNotBlank(matchExpression) && !BaseRulesBuilder.NOTAPARTITIONTABLE
            .equalsIgnoreCase(matchExpression);
        Query subQuery = QueryUtil.simpleQuery(
            QueryUtil.selectList(baseFunction.arguments()),
            QueryUtil.table(fromTable),
            usePartition ? dqc : null,
            new GroupBy(false, toGroupElement(baseFunction.arguments())),
            count
            , null, null, null
        );
        Query query = QueryUtil.simpleQuery(
            QueryUtil.selectList(new FunctionCall(QualifiedName.of("count"), false, ImmutableList.of())),
            new AliasedRelation(
                new TableSubQuery(subQuery),
                new Identifier("t")
            )
        );
        DialectNode dialectNode = MaxComputeFormatter.formatSql(query, MaxComputeContext.builder().build());
        return dialectNode.getNode();
    }

    private List<GroupingElement> toGroupElement(List<BaseExpression> arguments) {
        return ImmutableList.of(new SimpleGroupBy(arguments));
    }

    @Override
    public boolean isMatch(TableFunction baseFunction) {
        return baseFunction.funcName() == BaseFunctionName.UNIQUE;
    }
}
