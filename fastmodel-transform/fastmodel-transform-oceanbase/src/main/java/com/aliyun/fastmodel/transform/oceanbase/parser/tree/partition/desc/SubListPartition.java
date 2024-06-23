package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubListPartition extends BaseSubPartition {

    private final BaseExpression expression;

    private final List<Identifier> columnList;

    public SubListPartition(BaseExpression expression, List<Identifier> columnList) {
        this.columnList = columnList;
        this.expression = expression;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> extensionAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubListPartition(this, context);
    }
}
