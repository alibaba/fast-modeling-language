/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.Lists;

/**
 * MaxComputeGenericDataType
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public class MaxComputeGenericDataType extends GenericDataType {

    public MaxComputeGenericDataType(NodeLocation location, String origin, String dataTypeName,
                                     List<DataTypeParameter> arguments) {
        super(location, origin, dataTypeName, arguments);
    }

    public MaxComputeGenericDataType(Identifier name,
                                     List<DataTypeParameter> arguments) {
        super(name, arguments);
    }

    public MaxComputeGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    public MaxComputeGenericDataType(IDataTypeName dataTypeName, DataTypeParameter... arguments) {
        this(dataTypeName.getValue(), arguments != null ? Lists.newArrayList(arguments) : null);
    }

    @Override
    public IDataTypeName getTypeName() {
        return MaxComputeDataTypeName.getByValue(this.getName());
    }
}
