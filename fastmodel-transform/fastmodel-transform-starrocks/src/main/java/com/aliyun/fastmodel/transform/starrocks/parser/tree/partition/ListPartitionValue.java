package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
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
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitListPartitionValue(this, context);
    }
}
