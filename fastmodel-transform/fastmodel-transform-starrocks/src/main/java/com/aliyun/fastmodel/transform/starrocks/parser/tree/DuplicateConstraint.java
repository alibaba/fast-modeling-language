package com.aliyun.fastmodel.transform.starrocks.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;

/**
 * aggregate constraint
 *
 * @author panguanjing
 * @date 2023/9/11
 */
@Getter
public class DuplicateConstraint extends CustomConstraint {

    public static final String TYPE = "DUPLICATE";

    private final List<Identifier> columns;

    public DuplicateConstraint(Identifier constraintName, List<Identifier> columns, Boolean enable) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public DuplicateConstraint(Identifier constraintName, List<Identifier> columns) {
        this(constraintName, columns, true);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksAstVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksAstVisitor.visitDuplicateConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }
}
