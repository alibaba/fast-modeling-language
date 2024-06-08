package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * multi range partition
 *
 * @author panguanjing
 * @date 2023/9/13
 */
@Getter
public class MultiRangePartition extends PartitionDesc {
    private final StringLiteral start;
    private final StringLiteral end;
    private final IntervalLiteral intervalLiteral;
    private final LongLiteral longLiteral;

    public MultiRangePartition(StringLiteral start, StringLiteral end, IntervalLiteral intervalLiteral,
        LongLiteral longLiteral) {
        this.start = start;
        this.end = end;
        this.intervalLiteral = intervalLiteral;
        this.longLiteral = longLiteral;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (start != null) {
            builder.add(start);
        }
        if (end != null) {
            builder.add(end);
        }
        if (intervalLiteral != null) {
            builder.add(intervalLiteral);
        }
        if (longLiteral != null) {
            builder.add(longLiteral);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitMultiRangePartition(this, context);
    }
}
