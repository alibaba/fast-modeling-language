/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLExpressionVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;
import lombok.Getter;

/**
 * HologresGenericDataType
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
public class PostgreSQLGenericDataType extends GenericDataType {

    public PostgreSQLGenericDataType(String dataTypeName) {
        super(dataTypeName);
    }

    public PostgreSQLGenericDataType(String dataTypeName, List<DataTypeParameter> arguments) {
        super(dataTypeName, arguments);
    }

    @Override
    public IDataTypeName getTypeName() {
        return PostgreSQLDataTypeName.getByValue(getName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        PostgreSQLVisitor<R, C> hologresVisitor = (PostgreSQLVisitor<R, C>)visitor;
        return hologresVisitor.visitPostgreSQLGenericDataType(this, context);
    }

    @Override
    public String toString() {
        return new PostgreSQLExpressionVisitor(PostgreSQLTransformContext.builder().build()).process(this);
    }
}
