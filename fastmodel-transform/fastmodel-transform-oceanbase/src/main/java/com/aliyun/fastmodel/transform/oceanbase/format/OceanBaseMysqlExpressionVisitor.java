package com.aliyun.fastmodel.transform.oceanbase.format;

import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlCharDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.OceanBaseMysqlGenericDataType;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * DorisExpressionVisitor
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class OceanBaseMysqlExpressionVisitor extends DefaultExpressionVisitor implements OceanBaseMysqlAstVisitor<String, Void> {
    public OceanBaseMysqlExpressionVisitor(OceanBaseContext context) {}

    @Override
    public String visitTableOrColumn(TableOrColumn tableOrColumn, Void context) {
        return super.visitTableOrColumn(tableOrColumn, context);
    }

    @Override
    public String visitOceanBaseCharDataType(OceanBaseMysqlCharDataType oceanBaseMysqlCharDataType, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        String dataTypeName = oceanBaseMysqlCharDataType.getDataTypeName();
        stringBuilder.append(dataTypeName);
        if (CollectionUtils.isNotEmpty(oceanBaseMysqlCharDataType.getArguments())) {
            stringBuilder.append(oceanBaseMysqlCharDataType.getArguments().stream()
                .map(this::process)
                .collect(Collectors.joining(",", "(", ")")));
        }
        if (oceanBaseMysqlCharDataType.getCharsetKey() != null) {
            stringBuilder.append(StringUtils.SPACE + oceanBaseMysqlCharDataType.getCharsetKey().getValue());
        }
        if (oceanBaseMysqlCharDataType.getCharsetName() != null) {
            stringBuilder.append(StringUtils.SPACE + oceanBaseMysqlCharDataType.getCharsetName());
        }
        if (oceanBaseMysqlCharDataType.getCollation() != null) {
            stringBuilder.append(" COLLATE ");
            stringBuilder.append(oceanBaseMysqlCharDataType.getCollation());
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitOceanBaseMysqlGenericDataType(OceanBaseMysqlGenericDataType node, Void context) {
        StringBuilder result = new StringBuilder();
        IDataTypeName typeName = node.getTypeName();
        result.append(typeName.getValue());
        boolean argNotEmpty = node.getArguments() != null && !node.getArguments().isEmpty();
        if (typeName.getDimension() == Dimension.MULTIPLE) {
            if (argNotEmpty) {
                result.append(node.getArguments().stream()
                    .map(this::process)
                    .collect(Collectors.joining(",", "<", ">")));
            }
        } else {
            if (argNotEmpty) {
                result.append(node.getArguments().stream()
                    .map(this::process)
                    .collect(Collectors.joining(",", "(", ")")));
            }
        }
        if (node.getSignEnum() != null) {
            result.append(" ").append(node.getSignEnum().name());
        }
        if (BooleanUtils.isTrue(node.getZeroFill())) {
            result.append(" ZEROFILL");
        }
        return result.toString();
    }
}
