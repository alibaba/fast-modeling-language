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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;
import com.aliyun.fastmodel.transform.graph.domain.Vertex;
import com.aliyun.fastmodel.transform.graph.domain.table.TableVertex;
import com.google.auto.service.AutoService;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/12
 */
@BuilderAnnotation(values = {
    CreateTable.class,
    CreateAdsTable.class,
    CreateDwsTable.class,
    CreateDimTable.class,
    CreateFactTable.class}, dialect = DialectName.GRAPH)
@AutoService(StatementBuilder.class)
public class CreateTableBuilder implements StatementBuilder<TransformContext> {

    @Override
    public GenericDialectNode<Vertex> buildGenericNode(BaseStatement source, TransformContext context) {
        CreateTable createTable = (CreateTable)source;
        Vertex vertex = new TableVertex(createTable);
        return new GenericDialectNode<Vertex>(vertex);
    }
}
