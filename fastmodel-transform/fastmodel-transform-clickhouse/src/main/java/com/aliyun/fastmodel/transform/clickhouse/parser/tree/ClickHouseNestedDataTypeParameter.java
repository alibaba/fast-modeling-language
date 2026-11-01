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

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * ClickHouse Nested 类型的字段参数，形如 Nested(a String, b Int32) 中的 "a String"
 *
 * @author panguanjing
 */
@Getter
public class ClickHouseNestedDataTypeParameter extends DataTypeParameter {

    private final Identifier name;

    private final BaseDataType type;

    public ClickHouseNestedDataTypeParameter(Identifier name, BaseDataType type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(type);
    }
}
