package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * list partition element
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Getter
public class ListPartitionElement extends PartitionElement {
    private final QualifiedName qualifiedName;

    private final Boolean defaultExpr;

    private final List<BaseExpression> expressionList;

    private final LongLiteral num;

    private final Property property;

    private final SubPartitionList subPartitionList;

    public ListPartitionElement(QualifiedName qualifiedName, Boolean defaultExpr, List<BaseExpression> expressionList, LongLiteral num,
        Property property, SubPartitionList subPartitionList) {
        this.qualifiedName = qualifiedName;
        this.defaultExpr = defaultExpr;
        this.expressionList = expressionList;
        this.num = num;
        this.property = property;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitListPartitionElement(this, context);
    }
}
