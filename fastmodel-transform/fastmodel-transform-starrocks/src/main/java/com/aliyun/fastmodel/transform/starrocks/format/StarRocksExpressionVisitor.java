/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.starrocks.format;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;
import com.aliyun.fastmodel.transform.starrocks.parser.util.StarRocksReservedWordUtil;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import org.apache.commons.lang3.StringUtils;

/**
 * hologres的表达式格式化
 *
 * @author panguanjing
 * @date 2022/6/24
 */
public class StarRocksExpressionVisitor extends DefaultExpressionVisitor implements StarRocksAstVisitor<String, Void> {

    private final StarRocksContext starRocksContext;

    public StarRocksExpressionVisitor(StarRocksContext starRocksContext) {
        this.starRocksContext = starRocksContext;
    }

    @Override
    public String visitStarRocksGenericDataType(StarRocksGenericDataType starRocksGenericDataType, Void context) {
        return super.visitGenericDataType(starRocksGenericDataType, context);
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ?
            StripUtils.strip(node.getOrigin()) : node.getValue();
        if (!node.isDelimited()) {
            boolean reservedKeyWord = StarRocksReservedWordUtil.isReservedKeyWord(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord) {
                return StripUtils.addPrefix(value);
            }
            return value;
        } else {
            String strip = StripUtils.strip(value);
            return StripUtils.addPrefix(strip);
        }
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Void context) {
        return "NULL";
    }

    @Override
    public String formatStringLiteral(String s) {
        if (s == null) {
            return null;
        }
        String result = s.replace("\\", "\\\\");
        result = result.replace("\"", "\\\"");
        return "\"" + result + "\"";
    }
}
