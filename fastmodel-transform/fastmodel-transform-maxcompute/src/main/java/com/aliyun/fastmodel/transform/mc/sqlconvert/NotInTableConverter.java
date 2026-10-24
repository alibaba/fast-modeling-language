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

import java.util.Arrays;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.InPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.SubQueryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.sqlconvert.SqlConverter;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.google.common.collect.ImmutableList;

/**
 * InTable函数实现
 *
 * @author panguanjing
 * @date 2021/6/4
 */
public class NotInTableConverter implements SqlConverter<InTableFunction, MaxComputeContext> {

    /**
     * count function name
     */
    public static final String COUNT = "count";

    @Override
    public String convert(QualifiedName tableName, InTableFunction inTableFunction, String matchExpression,
                          MaxComputeContext transformContext) {
        //增加schema支持
        QualifiedName inTableFunctionTableName = StringJoinUtil.join(transformContext.getMcProject(), transformContext.getMcSchema(),
            inTableFunction.getTableName().getSuffix());
        QualifiedName fromTable = StringJoinUtil.join(transformContext.getMcProject(), transformContext.getMcSchema(), tableName.getSuffix());
        ;
        /**
         * select count(*) from 表1 inPredicate 列1 in (select 列1 from 表2 )
         */
        InPredicate inPredicate = new InPredicate(
            new TableOrColumn(inTableFunction.getMatchColumn()),
            new SubQueryExpression(
                QueryUtil.simpleQuery(
                    QueryUtil.selectList(
                        new TableOrColumn(QualifiedName.of(Arrays.asList(inTableFunction.getInTableColumn())))
                    ),
                    QueryUtil.table(inTableFunctionTableName)
                )
            )
        );
        Query query = QueryUtil.simpleQuery(
            QueryUtil.selectList(new FunctionCall(
                QualifiedName.of(COUNT),
                false,
                ImmutableList.of()
            )),
            QueryUtil.table(fromTable),
            new NotExpression(null, inPredicate));
        return MaxComputeFormatter.formatSql(query, transformContext).getNode();
    }

}
