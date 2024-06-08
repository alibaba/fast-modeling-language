package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;

/**
 * less than
 *
 * @author panguanjing
 * @date 2023/9/14
 */
@Getter
public class LessThanPartitionKey extends PartitionKey {

    private final boolean maxValue;

    private final ListPartitionValue partitionValues;

    public LessThanPartitionKey(boolean maxValue, ListPartitionValue partitionValues) {
        this.maxValue = maxValue;
        this.partitionValues = partitionValues;
    }

    public LessThanPartitionKey(ListPartitionValue partitionValues) {
        this.partitionValues = partitionValues;
        this.maxValue =false;
    }

    @Override
    public List<? extends Node> getChildren() {
        if (maxValue) {
            return ImmutableList.of();
        }
        return Lists.newArrayList(partitionValues);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitLessThanPartitionKey(this, context);
    }
}
