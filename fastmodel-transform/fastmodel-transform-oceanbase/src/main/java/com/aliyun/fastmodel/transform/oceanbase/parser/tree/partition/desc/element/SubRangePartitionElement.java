package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * range sub
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Getter
public class SubRangePartitionElement extends SubPartitionElement {
    private final SingleRangePartition singleRangePartition;

    public SubRangePartitionElement(SingleRangePartition singleRangePartition) {this.singleRangePartition = singleRangePartition;}

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(singleRangePartition);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitRangeSubPartitionElement(this, context);
    }
}
