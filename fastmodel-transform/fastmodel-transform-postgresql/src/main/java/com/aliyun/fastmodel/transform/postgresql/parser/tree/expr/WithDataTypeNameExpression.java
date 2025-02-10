package com.aliyun.fastmodel.transform.postgresql.parser.tree.expr;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;
import lombok.Getter;

/**
 * with dataTypeName Expression
 *
 * @author panguanjing
 * @date 2024/4/11
 */
@Getter
public class WithDataTypeNameExpression extends BaseExpression {

    private final BaseExpression baseExpression;

    private final BaseDataType baseDataType;

    public WithDataTypeNameExpression(NodeLocation location, BaseExpression baseExpression, BaseDataType dataTypeName) {
        super(location);
        this.baseExpression = baseExpression;
        this.baseDataType = dataTypeName;
    }

    public WithDataTypeNameExpression(NodeLocation location, String origin, BaseExpression baseExpression, BaseDataType dataTypeName) {
        super(location, origin);
        this.baseExpression = baseExpression;
        this.baseDataType = dataTypeName;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        PostgreSQLVisitor<R, C> hologresVisitor = (PostgreSQLVisitor<R, C>)visitor;
        return hologresVisitor.visitWithDataTypeNameExpression(this, context);
    }
}
