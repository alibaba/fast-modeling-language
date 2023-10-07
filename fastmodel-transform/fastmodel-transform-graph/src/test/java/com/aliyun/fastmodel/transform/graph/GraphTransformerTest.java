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

package com.aliyun.fastmodel.transform.graph;

import java.io.IOException;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.graph.context.GraphTransformContext;
import com.aliyun.fastmodel.transform.graph.exporter.ExportName;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public class GraphTransformerTest extends BaseTransformerTest {

    GraphTransformer graphTransformer;

    @Before
    public void setUp() throws Exception {
        graphTransformer = new GraphTransformer();
    }

    @Test
    public void transform() throws IOException {
        BaseStatement source = initBaseStatement();
        DialectNode transform = graphTransformer.transform(source,
            GraphTransformContext.builder().exportName(
                ExportName.JSON).build());
        String node = transform.getNode();
        assertEquals(expectFromFile("graph.json"), node);
    }

}