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

package com.aliyun.fastmodel.transform.graph.builder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;
import com.aliyun.fastmodel.transform.graph.domain.Edge;
import com.aliyun.fastmodel.transform.graph.domain.table.TableEdge;
import com.google.auto.service.AutoService;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

/**
 * 针对关联关系的builder处理
 *
 * @author panguanjing
 * @date 2021/12/15
 */
@BuilderAnnotation(values = {RefRelation.class}, dialect = DialectName.Constants.GRAPH)
@AutoService(StatementBuilder.class)
public class RefRelationBuilder implements StatementBuilder<TransformContext> {

    public static final String SEPARATOR = ",";

    @Override
    public GenericDialectNode<Edge> buildGenericNode(BaseStatement source, TransformContext context) {
        RefRelation relation = (RefRelation)source;
        RefObject left = relation.getLeft();
        RefObject right = relation.getRight();
        String identifier = relation.getIdentifier();
        String sourceVertex = left.getMainName().toString();
        Map<String, Object> maps = Maps.newHashMapWithExpectedSize(16);
        List<Identifier> columnList = relation.getLeft().getAttrNameList();
        if (CollectionUtils.isNotEmpty(columnList)) {
            maps.put(TableEdge.LEFT,
                Joiner.on(SEPARATOR).join(columnList.stream().map(Identifier::getValue).collect(Collectors.toList())));
        }
        List<Identifier> rightList = relation.getRight().getAttrNameList();
        if (CollectionUtils.isNotEmpty(rightList)) {
            maps.put(TableEdge.RIGHT,
                Joiner.on(SEPARATOR).join(columnList.stream().map(Identifier::getValue).collect(Collectors.toList())));
        }
        maps.put(TableEdge.LEFT_COMMENT, relation.getLeft().getCommentValue());
        maps.put(TableEdge.RIGHT_COMMENT, relation.getRight().getCommentValue());
        String targetVertex = right.getMainName().toString();
        TableEdge build = TableEdge.builder()
            .id(relation.getQualifiedName().toString())
            .label(identifier)
            .source(sourceVertex)
            .target(targetVertex)
            .maps(maps)
            .build();
        return new GenericDialectNode<>(build);
    }
}
