/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.postgresql.parser.visitor;

import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.DateLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLArrayDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLIntervalDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.expr.WithDataTypeNameExpression;
import com.aliyun.fastmodel.transform.postgresql.parser.util.PostgreSQLReservedWordUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * hologres的表达式格式化
 *
 * @author panguanjing
 * @date 2022/6/24
 */
public class PostgreSQLExpressionVisitor extends DefaultExpressionVisitor implements PostgreSQLVisitor<String, Void> {

    private final PostgreSQLTransformContext hologresTransformContext;

    private final static Pattern pattern = Pattern.compile("^\\d\\w*");

    public PostgreSQLExpressionVisitor(PostgreSQLTransformContext hologresTransformContext) {
        this.hologresTransformContext = hologresTransformContext;
    }

    public static boolean startsWithNumber(String word) {
        return pattern.matcher(word).matches();
    }

    @Override
    public String visitPostgreSQLArrayDataType(PostgreSQLArrayDataType hologresArrayDataType, Void context) {
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
    public String visitPostgreSQLIntervalDataType(PostgreSQLIntervalDataType postgreSQLIntervalDataType, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(postgreSQLIntervalDataType.getTypeName().getValue());
        IntervalQualifiers from = postgreSQLIntervalDataType.getFrom();
        if (from != null) {
            stringBuilder.append(" ").append(from.getCode());
        }
        if (postgreSQLIntervalDataType.getTo() != null) {
            stringBuilder.append(" TO ").append(postgreSQLIntervalDataType.getTo().getCode());
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ?
            StripUtils.strip(node.getOrigin()) : node.getValue();
        if (!node.isDelimited()) {
            boolean reservedKeyWord = PostgreSQLReservedWordUtil.isReservedKeyWord(value);
            boolean startWithNumber = startsWithNumber(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord || startWithNumber) {
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

    @Override
    public String visitWithDataTypeNameExpression(WithDataTypeNameExpression withDataTypeNameExpression, Void context) {
        BaseExpression baseExpression = withDataTypeNameExpression.getBaseExpression();
        BaseDataType dataTypeName = withDataTypeNameExpression.getBaseDataType();
        if (dataTypeName != null) {
            String accept = dataTypeName.accept(this, context);
            return baseExpression.accept(this, context) + "::" + accept;
        }
        return baseExpression.accept(this, context);
    }

    @Override
    public String visitTimestampLiteral(TimestampLiteral node, Void context) {
        return node.getTimestampFormat();
    }

    @Override
    public String visitDateLiteral(DateLiteral dateLiteral, Void context) {
        return dateLiteral.getValue();
    }

    @Override
    public String visitCurrentTimestamp(CurrentTimestamp currentTimestamp, Void context) {
        return CURRENT_TIMESTAMP;
    }
}
