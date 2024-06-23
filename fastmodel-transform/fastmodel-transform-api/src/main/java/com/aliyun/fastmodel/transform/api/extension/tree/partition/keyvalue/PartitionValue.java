package com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * PartitionValue
 *
 * @author panguanjing
 * @date 2023/10/23
 */
@Getter
public class PartitionValue extends AbstractNode {

    private final boolean maxValue;

    private final BaseExpression stringLiteral;

    public PartitionValue(BaseExpression stringLiteral) {
        this(false, stringLiteral);
    }

    public PartitionValue(boolean maxValue, BaseExpression stringLiteral) {
        this.maxValue = maxValue;
        this.stringLiteral = stringLiteral;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitPartitionValue(this, context);
    }
}
