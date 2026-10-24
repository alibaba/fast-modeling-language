package com.aliyun.fastmodel.transform.mysql.client.converter;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.aliyun.fastmodel.transform.mysql.format.MysqlVisitor;
import com.aliyun.fastmodel.transform.mysql.parser.MysqlTransformerParser;
import com.aliyun.fastmodel.transform.mysql.parser.tree.MysqlDataTypeName;

/**
 * MySQL客户端转换器
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public class MysqlClientConverter extends ExtensionClientConverter<MysqlTransformContext> {

    private final MysqlTransformerParser mysqlTransformerParser;

    private final MysqlPropertyConverter mysqlPropertyConverter;

    public MysqlClientConverter() {
        mysqlTransformerParser = new MysqlTransformerParser();
        mysqlPropertyConverter = new MysqlPropertyConverter();
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.mysqlTransformerParser;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return mysqlPropertyConverter;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        try {
            return MysqlDataTypeName.getByValue(dataTypeName);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * 含 UNSIGNED / SIGNED / ZEROFILL 修饰符的类型，需要把修饰符放在参数之后，
     * 例如 INT(10) UNSIGNED，不能格式化为 INT UNSIGNED(10)
     *
     * @param column 列信息
     * @return 数据类型
     */
    @Override
    public BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (MysqlDataTypeName.hasModifier(dataTypeName)) {
            return getLanguageParser().parseDataType(formatModifierDataType(column), ReverseContext.builder().build());
        }
        return super.getDataType(column);
    }

    private String formatModifierDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (dataTypeName.contains("(") && column.getLength() == null
            && column.getPrecision() == null && column.getScale() == null) {
            return dataTypeName;
        }
        IDataTypeName typeName = MysqlDataTypeName.getByValue(dataTypeName);
        String base = MysqlDataTypeName.getBaseValue(dataTypeName);
        Dimension dimension = typeName.getDimension();
        StringBuilder result = new StringBuilder(base);
        if (dimension == Dimension.ONE) {
            Integer length = column.getLength();
            if (length != null && length > 0) {
                result.append("(").append(length).append(")");
            }
        } else if (dimension == Dimension.TWO && column.getPrecision() != null) {
            result.append("(").append(column.getPrecision());
            if (column.getScale() != null) {
                result.append(",").append(column.getScale());
            }
            result.append(")");
        }
        String modifiers = MysqlDataTypeName.getModifiers(dataTypeName);
        if (!modifiers.isEmpty()) {
            result.append(" ").append(modifiers);
        }
        return result.toString();
    }

    @Override
    protected BaseExpression toDefaultValueExpression(BaseDataType dataType, String defaultValue) {
        if ("NULL".equalsIgnoreCase(defaultValue)) {
            return new NullLiteral();
        }
        return super.toDefaultValueExpression(dataType, defaultValue);
    }

    @Override
    public String getRaw(Node node) {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());
        node.accept(mysqlVisitor, 0);
        return mysqlVisitor.getBuilder().toString();
    }
}
