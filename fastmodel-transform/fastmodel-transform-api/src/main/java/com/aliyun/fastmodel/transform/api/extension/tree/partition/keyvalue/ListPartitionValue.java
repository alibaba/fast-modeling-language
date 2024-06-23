package com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * list partition value
 *
 * @author panguanjing
 * @date 2023/10/23
 */
@Getter
public class ListPartitionValue extends AbstractNode {

    private final List<PartitionValue> partitionValueList;

    public ListPartitionValue(NodeLocation location,
        List<PartitionValue> partitionValueList) {
        super(location);
        this.partitionValueList = partitionValueList;
    }

    public ListPartitionValue(List<PartitionValue> partitionValueList) {this.partitionValueList = partitionValueList;}

    @Override
    public List<? extends Node> getChildren() {
        return partitionValueList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitListPartitionValue(this, context);
    }
}
