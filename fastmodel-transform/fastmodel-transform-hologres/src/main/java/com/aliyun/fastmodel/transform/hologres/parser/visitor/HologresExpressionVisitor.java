/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import java.util.List;
import java.util.StringJoiner;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * hologres的表达式格式化
 *
 * @author panguanjing
 * @date 2022/6/24
 */
public class HologresExpressionVisitor extends DefaultExpressionVisitor implements HologresVisitor<String, Void> {

    @Override
    public String visitHologresArrayDataType(HologresArrayDataType hologresArrayDataType, Void context) {
        List<ArrayBounds> dataTypeParameter = hologresArrayDataType.getDataTypeParameter();
        StringBuilder stringBuilder = new StringBuilder();
        if (CollectionUtils.isNotEmpty(dataTypeParameter)) {
            for (ArrayBounds arrayBounds : dataTypeParameter) {
                StringJoiner stringJoiner1 = new StringJoiner("", "[", "]");
                if (arrayBounds.getIndex() != null) {
                    stringJoiner1.add(String.valueOf(arrayBounds.getIndex()));
                }
                stringBuilder.append(stringJoiner1);
            }
            return hologresArrayDataType.getSource().getTypeName().getValue() + stringBuilder;
        }
        return hologresArrayDataType.getTypeName().getValue();
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ?
            StripUtils.strip(node.getOrigin()) : node.getValue();
        if (!node.isDelimited()) {
            return value;
        } else {
            String strip = StripUtils.strip(value);
            return "\"" + strip + "\"";
        }
    }
}
