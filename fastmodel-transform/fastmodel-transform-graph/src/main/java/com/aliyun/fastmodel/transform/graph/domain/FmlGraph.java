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

package com.aliyun.fastmodel.transform.graph.domain;

import java.util.List;

import com.google.common.collect.Lists;
import lombok.Data;

/**
 * Graph Data
 *
 * @author panguanjing
 * @date 2021/12/19
 */
@Data
public class FmlGraph {
    private List<Vertex> nodes;
    private List<Edge> edges;

    public void addEdge(Edge edge) {
        if (edges == null) {
            edges = Lists.newArrayList();
        }
        edges.add(edge);
    }

    public void addVertex(Vertex vertex) {
        if (nodes == null) {
            nodes = Lists.newArrayList();
        }
        nodes.add(vertex);
    }
}
