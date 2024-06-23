/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.doris.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.format.DorisExpressionVisitor;
import com.aliyun.fastmodel.transform.doris.parser.visitor.DorisAstVisitor;
import lombok.Getter;

/**
 * DorisGenericDataType
 *
 * @author panguanjing
 * @date 2024/01/20
 */
@Getter
public class DorisGenericDataType extends GenericDataType {

    public DorisGenericDataType(String dataTypeName) {
        super(dataTypeName);
    }

    public DorisGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    public DorisGenericDataType(NodeLocation location, String origin, String dataTypeName,
        List<DataTypeParameter> arguments) {
        super(location, origin, dataTypeName, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return DorisDataTypeName.getByValue(getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        DorisAstVisitor<R, C> astVisitor = (DorisAstVisitor<R, C>)visitor;
        return astVisitor.visitDorisGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new DorisExpressionVisitor(DorisContext.builder().build()).process(this);
    }
}
