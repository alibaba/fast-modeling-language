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
import java.util.Locale;
import java.util.Objects;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 通用数据类型
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
public class GenericDataType extends BaseDataType {

    private final String name;

    private final List<DataTypeParameter> arguments;

    public GenericDataType(Identifier name) {
        this(null, null, name.getValue(), ImmutableList.of());
    }

    public GenericDataType(String name) {
        this(null, null, name, ImmutableList.of());
    }

    public GenericDataType(NodeLocation location, String origin, String dataTypeName, List<DataTypeParameter> arguments) {
        super(location, origin);
        Preconditions.checkNotNull(dataTypeName);
        name = dataTypeName;
        this.arguments = arguments;
    }

    public String getName() {
        return name.toUpperCase(Locale.ROOT);
    }

    public GenericDataType(Identifier name, List<DataTypeParameter> arguments) {
        this(name.getValue(), arguments);
    }

    public GenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        this(null, null, dataTypeName, arguments);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitGenericDataType(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        GenericDataType that = (GenericDataType)o;
        boolean e = StringUtils.equalsIgnoreCase(this.getTypeName().getValue(), that.getTypeName().getValue());
        if (!e) {
            return false;
        }
        List<DataTypeParameter> arguments = this.getArguments();
        List<DataTypeParameter> arguments1 = that.getArguments();
        return Objects.equals(arguments, arguments1);
    }

    @Override
    public IDataTypeName getTypeName() {
        DataTypeEnums dataType = DataTypeEnums.getDataType(name);
        if (dataType != DataTypeEnums.CUSTOM) {
            return dataType;
        }
        return new IDataTypeName() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public String getValue() {
                return name;
            }
        };
    }

}
