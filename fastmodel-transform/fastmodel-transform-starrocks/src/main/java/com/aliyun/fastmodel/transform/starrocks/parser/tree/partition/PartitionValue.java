package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
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

    private final StringLiteral stringLiteral;

    public PartitionValue(StringLiteral stringLiteral) {
        this.stringLiteral = stringLiteral;
        this.maxValue = false;
    }

    public PartitionValue(boolean maxValue, StringLiteral stringLiteral) {
        this.maxValue = maxValue;
        this.stringLiteral = stringLiteral;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitPartitionValue(this, context);
    }
}
