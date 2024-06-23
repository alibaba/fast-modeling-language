package com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * list partition value
 *
 * @author panguanjing
 * @date 2023/9/14
 */
@Getter
public class ArrayPartitionKey extends PartitionKey {

    private final List<ListPartitionValue> partitionValues;

    public ArrayPartitionKey(List<ListPartitionValue> partitionValues) {this.partitionValues = partitionValues;}

    @Override
    public List<? extends Node> getChildren() {
        return partitionValues;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitArrayPartitionKey(this, context);
    }
}
