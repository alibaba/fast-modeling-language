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
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataType;
import com.aliyun.fastmodel.transform.hologres.parser.util.HologresReservedWordUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * hologres的表达式格式化
 *
 * @author panguanjing
 * @date 2022/6/24
 */
public class HologresExpressionVisitor extends DefaultExpressionVisitor implements HologresVisitor<String, Void> {

    private final HologresTransformContext hologresTransformContext;

    public HologresExpressionVisitor(HologresTransformContext hologresTransformContext) {
        this.hologresTransformContext = hologresTransformContext;
    }

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
            boolean reservedKeyWord = HologresReservedWordUtil.isReservedKeyWord(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord) {
                return StripUtils.addDoubleStrip(value);
            } else if (hologresTransformContext.isCaseSensitive()) {
                //如果开启了不忽略大小写，那么统一加上双引号
                return StripUtils.addDoubleStrip(value);
            }
            return value;
        } else {
            String strip = StripUtils.strip(value);
            return StripUtils.addDoubleStrip(strip);
        }
    }
}
