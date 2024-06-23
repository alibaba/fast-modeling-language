package com.aliyun.fastmodel.transform.api.extension.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * RangePartitionedBy
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
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitRangePartitionedBy(this, context);
    }
}
