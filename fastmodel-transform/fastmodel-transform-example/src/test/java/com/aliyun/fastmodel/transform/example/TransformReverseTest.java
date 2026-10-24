/*
 * Copyright 2021-2023 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.example;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 逆向的测试类
 *
 * @author panguanjing
 * @date 2023/8/7
 */
public class TransformReverseTest {

    Transformer transformer = TransformerFactory.getInstance().get(DialectMeta.DEFAULT_MAX_COMPUTE);

    @Test
    public void testReverse() {
        DialectNode dialectNode = new DialectNode("create table a (b bigint comment 'abc') comment 'bcd';");
        ReverseContext build = ReverseContext.builder().build();
        Node reverse = transformer.reverse(dialectNode, build);
        assertEquals("CREATE DIM TABLE a \n"
            + "(\n"
            + "   b BIGINT COMMENT 'abc'\n"
            + ")\n"
            + "COMMENT 'bcd'", reverse.toString());
    }

    @Test
    public void testTransformTable() {
        DialectNode dialectNode = new DialectNode("create table a (b bigint comment 'abc') comment 'bcd';");
        ReverseContext build = ReverseContext.builder().build();
        Node reverse = transformer.reverse(dialectNode, build);
        Table table = transformer.transformTable(reverse, TransformContext.builder().build());
        assertEquals("a", table.getName());
        assertEquals(1, table.getColumns().size());
    }
}
