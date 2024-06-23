package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * check expression constraint
 *
 * @author panguanjing
 * @date 2024/2/17
 */
@Getter
public class CheckExpressionConstraint extends CustomConstraint {

    public static final String CHECK = "CHECK";

    private final BaseExpression expression;

    private final Boolean enforced;

    public CheckExpressionConstraint(Identifier constraintName, Boolean enable, String customType, BaseExpression expression, Boolean enforced) {
        super(constraintName, enable, customType);
        this.expression = expression;
        this.enforced = enforced;
    }

    public CheckExpressionConstraint(Identifier constraintName, BaseExpression expression, Boolean enforced) {
        this(constraintName, true, CHECK, expression, enforced);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitCheckExpressionConstraint(this, context);
    }
}
