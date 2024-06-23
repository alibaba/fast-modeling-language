package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;

/**
 * sub partition
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public abstract class BaseSubPartition extends AbstractNode {

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> extensionAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitBaseSubPartition(this, context);
    }
}
