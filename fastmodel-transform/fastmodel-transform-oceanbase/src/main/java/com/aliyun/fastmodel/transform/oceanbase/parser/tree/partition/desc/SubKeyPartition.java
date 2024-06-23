package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * sub key partition
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubKeyPartition extends BaseSubPartition {

    private final List<Identifier> columnList;

    private final LongLiteral subpartitionCount;

    public SubKeyPartition(List<Identifier> columnList, LongLiteral subpartitionCount) {
        Preconditions.checkNotNull(columnList, "columnList can't be null");
        Preconditions.checkArgument(columnList.size() > 0, "columnList must be set");
        this.columnList = columnList;
        this.subpartitionCount = subpartitionCount;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> nodes = ImmutableList.<Node>builder();
        nodes.addAll(columnList);
        if (subpartitionCount != null) {
            nodes.add(subpartitionCount);
        }
        return nodes.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> extensionAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubKeyPartition(this, context);
    }
}
