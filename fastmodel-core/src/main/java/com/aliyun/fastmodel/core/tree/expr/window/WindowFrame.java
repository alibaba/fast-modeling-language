
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

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.enums.WindowFrameType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import static java.util.Objects.requireNonNull;

/**
 * @author panguanjing
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class WindowFrame
    extends AbstractNode {

    private final WindowFrameType type;
    private final FrameBound start;
    private final FrameBound end;

    public WindowFrame(WindowFrameType type, FrameBound start, FrameBound end) {
        this(null, type, start, end);
    }

    public WindowFrame(NodeLocation location, WindowFrameType type, FrameBound start, FrameBound end) {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.start = requireNonNull(start, "start is null");
        this.end = end;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitWindowFrame(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(start);
        if (end != null) {
            nodes.add(end);
        }
        return nodes.build();
    }

}
