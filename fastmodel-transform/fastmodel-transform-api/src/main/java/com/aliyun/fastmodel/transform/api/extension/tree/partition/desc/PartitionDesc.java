package com.aliyun.fastmodel.transform.api.extension.tree.partition.desc;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;

/**
 * RangePartition
 *
 * @author panguanjing
 * @date 2023/9/13
 */
public abstract class PartitionDesc extends AbstractNode {

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitPartitionDesc(this, context);
    }
}
