/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksExpressionVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;

/**
 * StarRocksGenericDataType
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
public class StarRocksGenericDataType extends GenericDataType {

    public StarRocksGenericDataType(String dataTypeName) {
        super(dataTypeName);
    }

    public StarRocksGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    public StarRocksGenericDataType(NodeLocation location, String origin, String dataTypeName,
        List<DataTypeParameter> arguments) {
        super(location, origin, dataTypeName, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return StarRocksDataTypeName.getByValue(getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksAstVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksAstVisitor.visitStarRocksGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new StarRocksExpressionVisitor(StarRocksContext.builder().build()).process(this);
    }
}
