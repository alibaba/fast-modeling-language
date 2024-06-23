package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * base oceanbase partition by
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class OceanBaseListPartitionBy extends OceanBasePartitionBy {

    private final BaseSubPartition subPartition;

    private final BaseExpression baseExpression;

    private final List<ListPartitionElement> partitionElementList;

    public OceanBaseListPartitionBy(List<ColumnDefinition> columnDefinitions,
        BaseExpression baseExpression, LongLiteral partitionCount, BaseSubPartition subPartition, List<ListPartitionElement> partitionElementList) {
        super(columnDefinitions, partitionCount);
        this.subPartition = subPartition;
        this.baseExpression = baseExpression;
        this.partitionElementList = partitionElementList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> mysqlAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return mysqlAstVisitor.visitOceanBaseListPartitionBy(this, context);
    }
}
