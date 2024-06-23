package com.aliyun.fastmodel.core.tree.statement.table.index;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * index expr
 *
 * @author panguanjing
 * @date 2024/2/8
 */
@Getter
public class IndexExpr extends IndexSortKey {

    private final BaseExpression expression;

    private final SortType sortType;

    public IndexExpr(BaseExpression expression, SortType sortType) {
        this.expression = expression;
        this.sortType = sortType;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitIndexExpr(this, context);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIndexExpr(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }
}
