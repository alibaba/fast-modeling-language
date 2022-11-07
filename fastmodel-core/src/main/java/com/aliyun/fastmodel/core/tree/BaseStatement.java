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

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;

/**
 * 抽象的语句接口
 *
 * @author panguanjing
 * @date 2020/09/02
 */
@Getter
@Setter
public abstract class BaseStatement extends AbstractNode {

    protected String origin;

    protected IStatementType statementType;

    public BaseStatement() {
        this(null, null);
    }

    public BaseStatement(NodeLocation location) {
        super(location);
    }

    public BaseStatement(NodeLocation location, String origin) {
        super(location);
        this.origin = origin;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        try {
            return FastModelFormatter.formatNode(this);
        } catch (Exception ignore) {
            return getClass() + ":{statementType:" + statementType + ";origin:" + origin + "}";
        }
    }
}
