package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * sub range partition
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubRangePartition extends BaseSubPartition {

    private final BaseExpression expression;

    private final List<Identifier> columnList;

    private final List<RangePartitionElement> singleRangePartitionList;

    public SubRangePartition(BaseExpression expression, List<Identifier> columnList, List<RangePartitionElement> singleRangePartitionList) {
        this.singleRangePartitionList = singleRangePartitionList;
        Preconditions.checkArgument(expression != null || columnList != null, "expression or columnList need not null");
        this.expression = expression;
        this.columnList = columnList;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> nodes = ImmutableList.<Node>builder();
        if (expression != null) {
            nodes.add(expression);
        }
        if (columnList != null) {
            nodes.addAll(columnList);
        }
        if (singleRangePartitionList != null) {
            nodes.addAll(singleRangePartitionList);
        }
        return nodes.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> extensionAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubRangePartition(this, context);
    }
}
