package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
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
public class OceanBaseHashPartitionBy extends OceanBasePartitionBy {

    private final BaseExpression expression;
    private final BaseSubPartition subPartition;
    private final List<HashPartitionElement> hashPartitionElements;

    public OceanBaseHashPartitionBy(LongLiteral partitionCount,
        BaseExpression expression,
        BaseSubPartition subPartition, List<HashPartitionElement> hashPartitionElements) {
        super(partitionCount);
        this.expression = expression;
        this.subPartition = subPartition;
        this.hashPartitionElements = hashPartitionElements;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> mysqlAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return mysqlAstVisitor.visitOceanBaseHashPartitionBy(this, context);
    }
}
