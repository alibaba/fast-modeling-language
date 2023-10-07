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

package com.aliyun.fastmodel.core.tree.expr.window;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.FrameBoundType;
import com.aliyun.fastmodel.core.tree.expr.enums.WindowFrameType;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/25
 */
public class WindowTest {
    Window window =
        new Window(
            ImmutableList.of(new StringLiteral("11")),
            new OrderBy(ImmutableList.of(new SortItem(
                new Identifier("sort"),
                Ordering.ASC,
                NullOrdering.UNDEFINED
            ))), new WindowFrame(WindowFrameType.ROWS,
            new FrameBound(null, FrameBoundType.CURRENT_ROW, new StringLiteral("1")),
            new FrameBound(null, FrameBoundType.CURRENT_ROW, new StringLiteral("2")))
        );

    @Test
    public void getChildren() {
        List<? extends Node> children = window.getChildren();
        assertEquals(children.size(), 3);
    }

    @Test
    public void testToString() {
        String s = window.toString();
        assertTrue(s.contains("sort"));
    }
}