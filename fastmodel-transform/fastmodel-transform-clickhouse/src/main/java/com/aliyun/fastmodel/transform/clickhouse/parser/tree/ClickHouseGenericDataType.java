/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;

/**
 * click house generic dataType
 *
 * @author panguanjing
 * @date 2022/7/10
 */
public class ClickHouseGenericDataType extends GenericDataType {

    public ClickHouseGenericDataType(Identifier name) {
        super(name);
    }

    public ClickHouseGenericDataType(Identifier name, List<DataTypeParameter> arguments) {
        super(name, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return ClickHouseDataTypeName.getByValue(this.getName());
    }
}
