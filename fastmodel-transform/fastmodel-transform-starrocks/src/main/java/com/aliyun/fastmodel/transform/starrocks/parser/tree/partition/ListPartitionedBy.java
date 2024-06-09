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
public class ListPartitionedBy extends PartitionedBy {

    /**
     * rangePartitions
     */
    private final List<PartitionDesc> listPartitions;

    public ListPartitionedBy(List<ColumnDefinition> columnDefinitions,
        List<PartitionDesc> listPartitions) {
        super(columnDefinitions);
        this.listPartitions = listPartitions;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitListPartitionedBy(this, context);
    }
}
