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

package com.aliyun.fastmodel.core.tree;

import java.util.List;

/**
 * abstract Node
 *
 * @author panguanjing
 * @date 2020/10/29
 */
public abstract class AbstractNode implements Node {

    private final NodeLocation location;

    protected AbstractNode() {
        this(null);
    }

    protected AbstractNode(NodeLocation location) {
        this.location = location;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }

    /**
     * get children
     *
     * @return List
     */
    @Override
    public abstract List<? extends Node> getChildren();

    /**
     * 返回location
     *
     * @return {@link NodeLocation}
     */
    public NodeLocation getLocation() {
        return location;
    }

}
