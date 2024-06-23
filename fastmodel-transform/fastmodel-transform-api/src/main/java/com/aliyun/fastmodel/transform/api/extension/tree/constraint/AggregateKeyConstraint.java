package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * aggregate constraint
 *
 * @author panguanjing
 * @date 2023/9/11
 */
@Getter
public class AggregateKeyConstraint extends CustomConstraint {

    public static final String TYPE = "AGGREGATE";

    private final List<Identifier> columns;

    public AggregateKeyConstraint(Identifier constraintName, List<Identifier> columns, Boolean enable) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public AggregateKeyConstraint(Identifier constraintName, List<Identifier> columns) {
        this(constraintName, columns, true);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitAggregateConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }
}
