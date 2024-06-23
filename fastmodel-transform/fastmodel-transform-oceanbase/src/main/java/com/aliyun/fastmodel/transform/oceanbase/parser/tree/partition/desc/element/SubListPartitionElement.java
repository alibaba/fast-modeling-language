package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * list sub partition element
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Getter
public class SubListPartitionElement extends SubPartitionElement {

    private final QualifiedName qualifiedName;

    private final Boolean defaultListExpr;

    private final List<BaseExpression> expressionList;

    private final Property property;

    public SubListPartitionElement(QualifiedName qualifiedName, Boolean defaultListExpr, List<BaseExpression> expressionList, Property property) {
        this.qualifiedName = qualifiedName;
        this.defaultListExpr = defaultListExpr;
        this.expressionList = expressionList;
        this.property = property;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (qualifiedName != null) {
            builder.add(qualifiedName);
        }
        if (expressionList != null) {
            builder.addAll(expressionList);
        }
        if (property != null) {
            builder.add(property);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitListSubPartitionElement(this, context);
    }
}
