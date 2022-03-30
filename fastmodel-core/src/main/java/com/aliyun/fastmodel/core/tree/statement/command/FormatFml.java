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

package com.aliyun.fastmodel.core.tree.statement.command;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.BaseCommandStatement;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Format Fml
 *
 * @author panguanjing
 * @date 2022/1/10
 */
@EqualsAndHashCode
@Getter
public class FormatFml extends BaseCommandStatement {
    /**
     * 具体的文本
     */
    @Getter
    private final Optional<String> fml;

    public FormatFml(NodeLocation nodeLocation, List<Property> properties, Optional<String> fml) {
        super(nodeLocation, properties);
        this.fml = fml;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
