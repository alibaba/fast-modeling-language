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

package com.aliyun.fastmodel.transform.graph.exporter.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.graph.domain.FmlGraph;
import com.aliyun.fastmodel.transform.graph.domain.table.TableEdge;
import com.aliyun.fastmodel.transform.graph.domain.table.TableVertex;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/19
 */
public class JacksonExporterTest {

    JacksonExporter jsonExporter = new JacksonExporter();

    @Test
    public void exportGraph() {
        FmlGraph graph = new FmlGraph();
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("co1"))
                .build()
        );
        CreateTable abc = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .detailType(TableDetailType.NORMAL_DIM)
            .build();
        TableVertex vertex = new TableVertex(abc);
        graph.addVertex(vertex);

        graph.addEdge(TableEdge.builder().source(
            vertex.id()
        ).target(vertex.id()).build());
        String s = jsonExporter.exportGraph(graph);
        assertEquals("{\"edges\":[{\"attribute\":null,\"id\":null,\"label\":null,\"source\":\"abc\","
            + "\"target\":\"abc\"}],\"nodes\":[{\"attribute\":{\"columns\":[{\"colAlias\":null,\"colComment\":null,"
            + "\"colName\":\"co1\",\"colType\":null,\"isPartitionKey\":null,\"notNull\":null,\"primaryKey\":null}],"
            + "\"name\":null,\"comment\":null},\"id\":\"abc\",\"label\":\"abc\",\"vertexType\":\"DIM\"}]}", s);
    }
}