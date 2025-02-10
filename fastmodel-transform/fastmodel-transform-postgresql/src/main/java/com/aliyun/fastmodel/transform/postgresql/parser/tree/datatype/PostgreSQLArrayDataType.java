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
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLExpressionVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Hologres Array Data Type
 *
 * @author panguanjing
 * @date 2022/6/9
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class PostgreSQLArrayDataType extends BaseDataType {

    private final BaseDataType source;

    private final List<ArrayBounds> dataTypeParameter;

    public PostgreSQLArrayDataType(BaseDataType source,
        List<ArrayBounds> dataTypeParameter) {
        this.source = source;
        this.dataTypeParameter = dataTypeParameter;
    }

    @Override
    public IDataTypeName getTypeName() {
        return new PostgreSQLArrayDataTypeName(source.getTypeName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        PostgreSQLVisitor<R, C> postgreSQLVisitor = (PostgreSQLVisitor<R, C>)visitor;
        return postgreSQLVisitor.visitPostgreSQLArrayDataType(this, context);
    }

    @Override
    public String toString() {
        return new PostgreSQLExpressionVisitor(PostgreSQLTransformContext.builder().build()).process(this);
    }
}
