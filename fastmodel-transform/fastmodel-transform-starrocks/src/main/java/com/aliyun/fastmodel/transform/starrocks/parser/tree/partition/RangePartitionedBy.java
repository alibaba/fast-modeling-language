package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;

/**
 * StarRocksPartitionedBy
 *
 * @author panguanjing
 * @date 2023/9/13
 */
@Getter
public class RangePartitionedBy extends PartitionedBy {

    /**
     * rangePartitions
     */
    private final List<PartitionDesc> rangePartitions;

    public RangePartitionedBy(List<ColumnDefinition> columnDefinitions,
        List<PartitionDesc> rangePartitions) {
        super(columnDefinitions);
        this.rangePartitions = rangePartitions;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitRangePartitionedBy(this, context);
    }
}
