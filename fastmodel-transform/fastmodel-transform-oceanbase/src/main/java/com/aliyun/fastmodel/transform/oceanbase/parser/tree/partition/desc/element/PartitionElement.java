package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;

/**
 * partition elements
 *
 * @author panguanjing
 * @date 2024/2/7
 */
public abstract class PartitionElement extends AbstractNode {
    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitPartitionElement(this, context);
    }
}
