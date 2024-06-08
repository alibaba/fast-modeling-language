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
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresExpressionVisitor;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;
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
public class HologresArrayDataType extends BaseDataType {

    private final BaseDataType source;

    private final List<ArrayBounds> dataTypeParameter;

    public HologresArrayDataType(BaseDataType source,
                                 List<ArrayBounds> dataTypeParameter) {
        this.source = source;
        this.dataTypeParameter = dataTypeParameter;
    }

    @Override
    public IDataTypeName getTypeName() {
        return new HologresArrayDataTypeName(source.getTypeName());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HologresVisitor<R, C> hologresVisitor = (HologresVisitor<R, C>)visitor;
        return hologresVisitor.visitHologresArrayDataType(this, context);
    }

    @Override
    public String toString() {
        return new HologresExpressionVisitor(HologresTransformContext.builder().build()).process(this);
    }
}
