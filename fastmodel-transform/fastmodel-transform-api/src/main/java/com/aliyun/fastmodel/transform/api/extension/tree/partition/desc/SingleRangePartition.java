package com.aliyun.fastmodel.transform.api.extension.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionKey;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
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
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitSingleRangePartition(this, context);
    }
}
