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
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * ClickHouse 类型里的字符串参数，例如 DateTime('Asia/Shanghai')、DateTime64(3, 'UTC')
 *
 * @author panguanjing
 */
@Getter
public class ClickHouseStringDataTypeParameter extends DataTypeParameter {

    private final StringLiteral value;

    public ClickHouseStringDataTypeParameter(StringLiteral value) {
        this.value = value;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
