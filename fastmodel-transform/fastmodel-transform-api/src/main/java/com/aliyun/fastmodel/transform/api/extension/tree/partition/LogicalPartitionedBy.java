package com.aliyun.fastmodel.transform.api.extension.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;

/**
 * logical partition  by
 * @author panguanjing
 * @date 2025/10/24
 */
public class LogicalPartitionedBy extends PartitionedBy {

    public LogicalPartitionedBy(List<ColumnDefinition> columnDefinitions) {
        super(columnDefinitions);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitLogicPartitionedBy(this, context);
    }
}
