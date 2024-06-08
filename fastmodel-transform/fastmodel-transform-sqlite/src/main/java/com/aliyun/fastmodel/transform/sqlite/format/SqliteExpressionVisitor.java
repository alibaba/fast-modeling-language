package com.aliyun.fastmodel.transform.sqlite.format;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.formatter.ExpressionVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.apache.commons.lang3.StringUtils;

/**
 * sqlite expression visitor
 *
 * @author panguanjing
 * @date 2023/8/29
 */
public class SqliteExpressionVisitor extends ExpressionVisitor {

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        String value = StringUtils.isNotBlank(node.getOrigin()) ?
            StripUtils.strip(node.getOrigin()) : node.getValue();
        if (!node.isDelimited()) {
            return value;
        } else {
            String strip = StripUtils.strip(value);
            return StripUtils.addDoubleStrip(strip);
        }
    }
}
