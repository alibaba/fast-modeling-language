package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * base oceanbase partition by
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public abstract class OceanBasePartitionBy extends PartitionedBy {

    private final LongLiteral partitionCount;

    public OceanBasePartitionBy(List<ColumnDefinition> columnDefinitions, LongLiteral partitionCount) {
        super(columnDefinitions);
        this.partitionCount = partitionCount;
    }

    public OceanBasePartitionBy(LongLiteral partitionCount) {
        this(null, partitionCount);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> mysqlAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return mysqlAstVisitor.visitOceanBasePartitionBy(this, context);
    }
}
