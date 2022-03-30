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
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
public abstract class BaseCompositeStatementBuilder<T extends TransformContext> implements StatementBuilder {

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
        LinkedListMultimap<StatementType, BaseStatement> map = LinkedListMultimap.create();
        for (BaseStatement statement : statements) {
            map.put(statement.getStatementType(), statement);
        }
        List<BaseStatement> list = new ArrayList<>();
        for (StatementType statementType : map.keySet()) {
            LinkedListMultimap<QualifiedName, BaseOperatorStatement> arrayListMultimap = LinkedListMultimap.create();
            Collection<BaseStatement> o = (Collection)map.get(statementType);
            for (BaseStatement b : o) {
                if (!(b instanceof BaseOperatorStatement)) {
                    list.add(b);
                    continue;
                }
                BaseOperatorStatement baseOperatorStatement = (BaseOperatorStatement)b;
                arrayListMultimap.put(baseOperatorStatement.getQualifiedName(), baseOperatorStatement);
            }
            for (QualifiedName k : arrayListMultimap.keySet()) {
                Collection<BaseOperatorStatement> collection = arrayListMultimap.get(k);
                if (collection.size() == 1) {
                    list.addAll(collection);
                    continue;
                }
                int createSize = count(collection);
                //果多个create语句，如果没有create语句, 都是同一个标识，忽略合并
                if (createSize > 1 || createSize == 0) {
                    list.addAll(collection);
                    continue;
                }
                //那么将其他的所有的合并进行处理
                List<BaseOperatorStatement> result = merge(collection);
                list.addAll(result);
            }
        }
        return innerBuild(new CompositeStatement(list), (T)context);
    }

    private List<BaseOperatorStatement> merge(Collection<BaseOperatorStatement> collection) {
        List<BaseOperatorStatement> list = new ArrayList<>();
        CreateTable create = (CreateTable)collection.stream().filter(
            x -> {
                return x instanceof CreateTable;
            }
        ).findFirst().orElse(null);
        if (create == null) {
            list.addAll(collection);
            return list;
        }
        List<BaseOperatorStatement> other = collection.stream().filter(
            x -> {
                return !(x instanceof CreateTable);
            }
        ).collect(Collectors.toList());
        for (BaseOperatorStatement baseOperatorStatement : other) {
            MergeResult mergeResult = mergeOperatorStatement(create, baseOperatorStatement);
            if (!mergeResult.isMergeSuccess()) {
                list.add(baseOperatorStatement);
            } else {
                create = mergeResult.getCreateTable();
            }
        }
        list.add(create);
        return list;
    }

    private MergeResult mergeOperatorStatement(CreateTable create, BaseOperatorStatement baseOperatorStatement) {
        if (baseOperatorStatement instanceof SetTableComment) {
            return getMergeResult(create, (SetTableComment)baseOperatorStatement);

        } else if (baseOperatorStatement instanceof SetTableAliasedName) {
            return getMergeResult(create, (SetTableAliasedName)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof SetTableProperties) {
            return getMergeResult(create, (SetTableProperties)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof UnSetTableProperties) {
            return getMergeResult(create, (UnSetTableProperties)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof SetColComment) {
            return getMergeResult(create, (SetColComment)baseOperatorStatement);
        }
        return new MergeResult(create, false);
    }

    private MergeResult getMergeResult(CreateTable create, SetColComment baseOperatorStatement) {
        SetColComment setColComment = baseOperatorStatement;
        Identifier changeColumn = setColComment.getChangeColumn();
        Comment comment = setColComment.getComment();
        if (create.isColumnEmpty()) {
            return new MergeResult(create, false);
        }
        List<ColumnDefinition> sourceColumnDefinition = create.getColumnDefines();
        List<ColumnDefinition> merge = mergeColumn(sourceColumnDefinition, changeColumn, comment);
        CreateTable build = CreateTable.builder()
                                       .tableName(create.getQualifiedName())
                                       .aliasedName(create.getAliasedName())
                                       .detailType(create.getTableDetailType())
                                       .columns(merge)
                                       .partition(create.getPartitionedBy())
                                       .constraints(create.getConstraintStatements())
                                       .comment(create.getComment())
                                       .properties(create.getProperties())
                                       .createOrReplace(create.getCreateOrReplace())
                                       .tableIndex(create.getTableIndexList())
                                       .ifNotExist(create.isNotExists())
                                       .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, UnSetTableProperties baseOperatorStatement) {
        UnSetTableProperties unSetTableProperties = baseOperatorStatement;
        List<String> propertyKeys = unSetTableProperties.getPropertyKeys();
        List<Property> sourceProperty = create.getProperties();
        if (create.isPropertyEmpty()) {
            return new MergeResult(create, false);
        }
        Map<String, String> propertyMap = PropertyUtil.toMap(sourceProperty);
        for (String key : propertyKeys) {
            propertyMap.remove(key);
        }
        List<Property> result = PropertyUtil.toProperty(propertyMap);
        CreateTable build = CreateTable.builder()
                                       .tableName(create.getQualifiedName())
                                       .aliasedName(create.getAliasedName())
                                       .detailType(create.getTableDetailType())
                                       .columns(create.getColumnDefines())
                                       .partition(create.getPartitionedBy())
                                       .constraints(create.getConstraintStatements())
                                       .comment(create.getComment())
                                       .properties(result)
                                       .createOrReplace(create.getCreateOrReplace())
                                       .tableIndex(create.getTableIndexList())
                                       .ifNotExist(create.isNotExists())
                                       .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableProperties baseOperatorStatement) {
        SetTableProperties setTableProperties = baseOperatorStatement;
        List<Property> properties = setTableProperties.getProperties();
        List<Property> sourceProperties = create.getProperties();
        if (sourceProperties == null) {
            sourceProperties = properties;
        } else {
            Map<String, String> source = PropertyUtil.toMap(sourceProperties);
            for (Property p : properties) {
                source.put(p.getName(), p.getValue());
            }
            sourceProperties = PropertyUtil.toProperty(source);
        }
        CreateTable build = CreateTable.builder()
                                       .tableName(create.getQualifiedName())
                                       .aliasedName(create.getAliasedName())
                                       .detailType(create.getTableDetailType())
                                       .columns(create.getColumnDefines())
                                       .partition(create.getPartitionedBy())
                                       .constraints(create.getConstraintStatements())
                                       .comment(create.getComment())
                                       .properties(sourceProperties)
                                       .createOrReplace(create.getCreateOrReplace())
                                       .tableIndex(create.getTableIndexList())
                                       .ifNotExist(create.isNotExists())
                                       .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableAliasedName baseOperatorStatement) {
        SetTableAliasedName setTableAliasedName = baseOperatorStatement;
        CreateTable build = CreateTable.builder()
                                       .tableName(create.getQualifiedName())
                                       .aliasedName(setTableAliasedName.getAliasedName())
                                       .detailType(create.getTableDetailType())
                                       .columns(create.getColumnDefines())
                                       .partition(create.getPartitionedBy())
                                       .constraints(create.getConstraintStatements())
                                       .comment(create.getComment())
                                       .properties(create.getProperties())
                                       .createOrReplace(create.getCreateOrReplace())
                                       .ifNotExist(create.isNotExists())
                                       .tableIndex(create.getTableIndexList())
                                       .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableComment baseOperatorStatement) {
        SetTableComment setTableComment = baseOperatorStatement;
        Comment comment = setTableComment.getComment();
        CreateTable build = CreateTable.builder()
                                       .tableName(create.getQualifiedName())
                                       .aliasedName(create.getAliasedName())
                                       .detailType(create.getTableDetailType())
                                       .columns(create.getColumnDefines())
                                       .partition(create.getPartitionedBy())
                                       .constraints(create.getConstraintStatements())
                                       .comment(comment)
                                       .properties(create.getProperties())
                                       .tableIndex(create.getTableIndexList())
                                       .createOrReplace(create.getCreateOrReplace())
                                       .ifNotExist(create.isNotExists())
                                       .build();
        return new MergeResult(build, true);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    static class MergeResult {
        private CreateTable createTable;
        private boolean mergeSuccess;
    }

    private List<ColumnDefinition> mergeColumn(List<ColumnDefinition> sourceColumnDefinition, Identifier changeColumn,
                                               Comment comment) {
        List<ColumnDefinition> list = Lists.newArrayListWithCapacity(sourceColumnDefinition.size());
        for (ColumnDefinition columnDefinition : sourceColumnDefinition) {
            if (!columnDefinition.getColName().equals(changeColumn)) {
                list.add(columnDefinition);
                continue;
            }
            ColumnDefinition newColumn = ColumnDefinition.builder()
                                                         .colName(columnDefinition.getColName())
                                                         .aliasedName(columnDefinition.getAliasedName())
                                                         .category(columnDefinition.getCategory())
                                                         .dataType(columnDefinition.getDataType())
                                                         .comment(comment)
                                                         .properties(columnDefinition.getColumnProperties())
                                                         .notNull(columnDefinition.getNotNull())
                                                         .primary(columnDefinition.getPrimary())
                                                         .defaultValue(columnDefinition.getDefaultValue())
                                                         .refDimension(columnDefinition.getRefDimension())
                                                         .refIndicators(columnDefinition.getRefIndicators())
                                                         .build();
            list.add(newColumn);
        }
        return list;
    }

    private int count(Collection<BaseOperatorStatement> collection) {
        return (int)collection.stream().filter(x -> {
            return x instanceof BaseCreate;
        }).count();
    }
}
