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

package com.aliyun.fastmodel.core.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 通用数据类型
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class GenericDataType extends BaseDataType {

    private final Identifier name;

    private final List<DataTypeParameter> arguments;

    public GenericDataType(Identifier dataTypeName) {
        this(dataTypeName, ImmutableList.of());
    }

    public GenericDataType(NodeLocation location, String origin,
                           Identifier dataTypeName,
                           List<DataTypeParameter> arguments) {
        super(location, origin);
        Preconditions.checkNotNull(dataTypeName);
        name = dataTypeName;
        this.arguments = arguments;
    }

    public GenericDataType(Identifier dataTypeName, List<DataTypeParameter> arguments) {
        this(null, null, dataTypeName, arguments);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.<Node>builder().add(name).addAll(arguments).build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGenericDataType(this, context);
    }

    @Override
    public DataTypeEnums getTypeName() {
        return DataTypeEnums.getDataType(name.getValue());
    }

}
