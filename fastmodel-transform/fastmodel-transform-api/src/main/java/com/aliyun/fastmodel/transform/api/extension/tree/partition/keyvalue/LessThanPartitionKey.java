package com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
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
        this.maxValue = false;
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
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitLessThanPartitionKey(this, context);
    }
}
