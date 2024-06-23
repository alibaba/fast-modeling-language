package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * RangePartitionElement
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class RangePartitionElement extends PartitionElement {

    private final LongLiteral idCount;

    private final SingleRangePartition singleRangePartition;

    private final SubPartitionList subPartitionList;

    public RangePartitionElement(LongLiteral idCount, SingleRangePartition singleRangePartition, SubPartitionList subPartitionList) {
        this.idCount = idCount;
        this.singleRangePartition = singleRangePartition;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> immutableList = new Builder();
        if (singleRangePartition != null) {
            immutableList.add(singleRangePartition);
        }
        if (subPartitionList != null) {
            immutableList.add(subPartitionList);
        }
        if (idCount != null) {
            immutableList.add(idCount);
        }
        return immutableList.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitRangePartitionElement(this, context);
    }
}
