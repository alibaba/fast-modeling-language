package com.aliyun.fastmodel.transform.postgresql.client.converter;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.util.StringLiteralUtil;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * PostgreSQLClientConverter
 *
 * @author panguanjing
 * @date 2024/10/20
 */
public abstract class PostgreSQLClientConverter<T extends PostgreSQLTransformContext> extends ExtensionClientConverter<T> {

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return PostgreSQLDataTypeName.getByValue(dataTypeName);
    }

    @Override
    protected BaseExpression toDefaultValueExpression(BaseDataType baseDataType, String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        IDataTypeName typeName = baseDataType.getTypeName();
        String type = typeName.getValue();
        if (StringUtils.equalsIgnoreCase(PostgreSQLDataTypeName.TIMESTAMPTZ.getValue(), type)) {
            return new TimestampLiteral(defaultValue);
        }
        BaseExpression baseExpression = super.getBaseExpression(baseDataType, defaultValue);
        if (baseExpression != null) {
            return baseExpression;
        }
        return getDefaultExpression(defaultValue);
    }

    protected BaseExpression getDefaultExpression(String defaultValue) {
        if (defaultValue.contains("::")) {
            return (BaseExpression)getLanguageParser().parseExpression(defaultValue);
        } else {
            String strip = StringLiteralUtil.strip(defaultValue);
            return new StringLiteral(strip);
        }
    }
}
