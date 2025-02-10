package com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLExpressionVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;
import lombok.Getter;

/**
 * IntervalDataType
 *
 * @author panguanjing
 * @date 2024/10/20
 */
@Getter
public class PostgreSQLIntervalDataType extends BaseDataType {

    private final IntervalQualifiers from;
    private final IntervalQualifiers to;

    public PostgreSQLIntervalDataType(IntervalQualifiers from, IntervalQualifiers to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public IDataTypeName getTypeName() {
        return PostgreSQLDataTypeName.INTERVAL;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        PostgreSQLVisitor<R, C> hologresVisitor = (PostgreSQLVisitor<R, C>)visitor;
        return hologresVisitor.visitPostgreSQLIntervalDataType(this, context);
    }

    @Override
    public String toString() {
        return new PostgreSQLExpressionVisitor(PostgreSQLTransformContext.builder().build()).process(this);
    }
}
