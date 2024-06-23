package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * base oceanbase partition by
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class OceanBaseKeyPartitionBy extends OceanBasePartitionBy {

    private final BaseSubPartition subPartition;

    private final List<HashPartitionElement> hashPartitionElements;

    public OceanBaseKeyPartitionBy(List<ColumnDefinition> columnDefinitions,
        LongLiteral partitionCount, BaseSubPartition subPartition, List<HashPartitionElement> hashPartitionElements) {
        super(columnDefinitions, partitionCount);
        this.subPartition = subPartition;
        this.hashPartitionElements = hashPartitionElements;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> mysqlAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return mysqlAstVisitor.visitOceanBaseKeyPartitionBy(this, context);
    }
}
