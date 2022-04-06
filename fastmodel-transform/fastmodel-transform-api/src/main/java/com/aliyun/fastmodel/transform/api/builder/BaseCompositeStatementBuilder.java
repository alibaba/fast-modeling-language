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

package com.aliyun.fastmodel.transform.api.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IStatementType;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.builder.merge.MergeBuilder;
import com.aliyun.fastmodel.transform.api.builder.merge.MergeResult;
import com.aliyun.fastmodel.transform.api.builder.merge.impl.CreateTableMergeBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.LinkedListMultimap;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
public abstract class BaseCompositeStatementBuilder<T extends TransformContext> implements StatementBuilder {

    private final MergeBuilder mergeBuilder = new CreateTableMergeBuilder();

    /**
     * 用于具体每个引擎来做到构建实现
     *
     * @param compositeStatement
     * @param context
     * @return
     */
    public abstract DialectNode innerBuild(CompositeStatement compositeStatement, T context);

    @Override
    public DialectNode build(BaseStatement source, TransformContext context) {
        CompositeStatement compositeStatement = (CompositeStatement)source;
        List<BaseStatement> statements = compositeStatement.getStatements();
        //按照statementType进行分组
        LinkedListMultimap<IStatementType, BaseStatement> map = LinkedListMultimap.create();
        for (BaseStatement statement : statements) {
            map.put(statement.getStatementType(), statement);
        }
        List<BaseStatement> list = new ArrayList<>();
        for (IStatementType statementType : map.keySet()) {
            LinkedListMultimap<QualifiedName, BaseStatement> arrayListMultimap = LinkedListMultimap.create();
            Collection<BaseStatement> o = map.get(statementType);
            for (BaseStatement b : o) {
                if (!(b instanceof BaseOperatorStatement)) {
                    list.add(b);
                    continue;
                }
                BaseOperatorStatement baseOperatorStatement = (BaseOperatorStatement)b;
                arrayListMultimap.put(baseOperatorStatement.getQualifiedName(), baseOperatorStatement);
            }
            for (QualifiedName k : arrayListMultimap.keySet()) {
                List<BaseStatement> collection = arrayListMultimap.get(k);
                if (collection.size() == 1) {
                    list.addAll(collection);
                    continue;
                }
                int createSize = count(collection);
                //如果多个create语句，如果没有create语句, 都是同一个标识，忽略合并
                if (createSize > 1 || createSize == 0) {
                    list.addAll(collection);
                    continue;
                }
                //那么将其他的所有的合并进行处理
                CompositeStatement compositeStatement2 = new CompositeStatement(collection);
                BaseStatement result = mergeBuilder.merge(compositeStatement2);
                if (result instanceof CompositeStatement) {
                    CompositeStatement compositeStatement1 = (CompositeStatement)result;
                    list.addAll(compositeStatement1.getStatements());
                } else {
                    list.add(result);
                }
            }
        }
        return innerBuild(new CompositeStatement(list), (T)context);
    }

    private int count(Collection<BaseStatement> collection) {
        return (int)collection.stream().filter(x -> x instanceof BaseCreate).count();
    }
}
