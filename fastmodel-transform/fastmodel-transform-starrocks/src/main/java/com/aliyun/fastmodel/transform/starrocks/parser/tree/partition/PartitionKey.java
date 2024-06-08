package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;

/**
 * partition key
 *
 * @author panguanjing
 * @date 2023/9/13
 */
public abstract class PartitionKey extends AbstractNode {

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitPartitionKey(this, context);
    }
}
