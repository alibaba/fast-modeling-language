/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresExpressionVisitor;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;
import lombok.Getter;
import lombok.ToString;

/**
 * HologresGenericDataType
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
public class HologresGenericDataType extends GenericDataType {

    public HologresGenericDataType(String dataTypeName) {
        super(dataTypeName);
    }

    public HologresGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return HologresDataTypeName.getByValue(getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HologresVisitor<R, C> hologresVisitor = (HologresVisitor<R, C>)visitor;
        return hologresVisitor.visitHologresGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new HologresExpressionVisitor(HologresTransformContext.builder().build()).process(this);
    }
}
