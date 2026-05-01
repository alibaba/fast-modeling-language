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

package com.aliyun.fastmodel.transform.hive.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.hive.format.HiveExpressionVisitor;
import com.aliyun.fastmodel.transform.hive.parser.visitor.HiveVisitor;
import com.google.common.collect.Lists;

/**
 * HiveGenericDataType
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public class HiveGenericDataType extends GenericDataType {

    public HiveGenericDataType(NodeLocation location, String origin, String dataTypeName, List<DataTypeParameter> arguments) {
        super(location, origin, dataTypeName, arguments);
    }

    public HiveGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    public HiveGenericDataType(IDataTypeName dataTypeName, DataTypeParameter... arguments) {
        this(dataTypeName.getValue(), arguments != null ? Lists.newArrayList(arguments) : null);
    }

    @Override
    public IDataTypeName getTypeName() {
        return HiveDataTypeName.getByValue(this.getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HiveVisitor<R, C> hiveVisitor = (HiveVisitor<R, C>)visitor;
        return hiveVisitor.visitHiveGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new HiveExpressionVisitor().process(this);
    }
}
