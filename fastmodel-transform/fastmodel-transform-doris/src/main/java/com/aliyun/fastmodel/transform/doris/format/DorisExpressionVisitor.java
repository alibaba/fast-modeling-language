package com.aliyun.fastmodel.transform.doris.format;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisGenericDataType;
import com.aliyun.fastmodel.transform.doris.parser.util.DorisReservedWordUtil;
import com.aliyun.fastmodel.transform.doris.parser.visitor.DorisAstVisitor;
import org.apache.commons.lang3.StringUtils;

/**
 * DorisExpressionVisitor
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class DorisExpressionVisitor extends DefaultExpressionVisitor implements DorisAstVisitor<String, Void> {
    public DorisExpressionVisitor(DorisContext context) {}

    @Override
    public String visitDorisGenericDataType(DorisGenericDataType dorisGenericDataType, Void context) {
        return DorisAstVisitor.super.visitDorisGenericDataType(dorisGenericDataType, context);
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ?
            StripUtils.strip(node.getOrigin()) : node.getValue();
        if (!node.isDelimited()) {
            boolean reservedKeyWord = DorisReservedWordUtil.isReservedKeyWord(value);
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
    public String formatStringLiteral(String s) {
        if (s == null) {
            return null;
        }
        String result = s.replace("\\", "\\\\");
        result = result.replace("\"", "\\\"");
        return "\"" + result + "\"";
    }
}
