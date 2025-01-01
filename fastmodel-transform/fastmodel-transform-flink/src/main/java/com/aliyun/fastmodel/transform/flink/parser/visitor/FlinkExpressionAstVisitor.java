package com.aliyun.fastmodel.transform.flink.parser.visitor;

import java.util.List;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRawDataType;

import static java.util.stream.Collectors.joining;

/**
 * @author 子梁
 * @date 2024/5/17
 */
public class FlinkExpressionAstVisitor extends DefaultExpressionVisitor implements FlinkAstVisitor<String, Void> {

    private final FlinkTransformContext flinkTransformContext;

    public FlinkExpressionAstVisitor(FlinkTransformContext flinkTransformContext) {
        this.flinkTransformContext = flinkTransformContext;
    }

    @Override
    public String visitRowDataType(RowDataType rowDataType, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append(rowDataType.getTypeName().getName());
        if (rowDataType.getFields() != null && !rowDataType.getFields().isEmpty()) {
            builder.append("<");
            List<Field> fields = rowDataType.getFields();
            String s = fields.stream().map(x -> formatField(x)).collect(joining(","));
            builder.append(s);
            builder.append(">");
        }
        return builder.toString();
    }

    @Override
    public String visitFlinkRawDataType(FlinkRawDataType flinkRawDataType, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append(flinkRawDataType.getTypeName().getName());
        if (flinkRawDataType.getQualifiedNames() != null && !flinkRawDataType.getQualifiedNames().isEmpty()) {
            builder.append("(");
            List<QualifiedName> qualifiedNames = flinkRawDataType.getQualifiedNames();
            String s = qualifiedNames.stream().map(x -> StripUtils.addStrip(x.toString())).collect(joining(","));
            builder.append(s);
            builder.append(")");
        }
        return builder.toString();
    }

    private String formatField(Field field) {
        StringBuilder s = new StringBuilder(process(field.getName()));
        s.append(" ").append(process(field.getDataType()));
        if (field.getComment() != null) {
            s.append(" COMMENT ").append(formatStringLiteral(field.getComment().getComment()));
        }
        return s.toString();
    }

}
