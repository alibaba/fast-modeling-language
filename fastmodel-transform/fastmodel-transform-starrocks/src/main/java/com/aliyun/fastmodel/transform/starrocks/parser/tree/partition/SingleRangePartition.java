package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * single range partition
 *
 * @author panguanjing
 * @date 2023/9/13
 */
@Getter
public class SingleRangePartition extends PartitionDesc {
    private final Identifier name;
    private final boolean ifNotExists;
    private final PartitionKey partitionKey;
    private final List<Property> propertyList;

    public SingleRangePartition(Identifier name, boolean ifNotExists, PartitionKey partitionKey, List<Property> propertyList) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.partitionKey = partitionKey;
        this.propertyList = propertyList;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (name != null) {
            builder.add(name);
        }
        if (partitionKey != null) {
            builder.add(partitionKey);
        }
        if (propertyList != null) {
            builder.addAll(propertyList);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitSingleRangePartition(this, context);
    }
}
