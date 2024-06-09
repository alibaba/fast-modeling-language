package com.aliyun.fastmodel.transform.starrocks.parser.tree.partition;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * StarRocksPartitionedBy
 *
 * @author 子梁
 * @date 2023/12/26
 */
@Getter
public class ExpressionPartitionBy extends PartitionedBy {

    /**
     * 函数表达式
     */
    private FunctionCall functionCall;

    /**
     * rangePartitions
     */
    private final List<PartitionDesc> rangePartitions;

    public ExpressionPartitionBy(List<ColumnDefinition> columnDefinitions,
                                 FunctionCall functionCall,
                                 List<PartitionDesc> rangePartitions) {
        super(columnDefinitions);
        this.functionCall = functionCall;
        this.rangePartitions = rangePartitions;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksVisitor.visitExpressionPartitionedBy(this, context);
    }

    public StringLiteral getTimeUnitArg() {
        if (this.functionCall == null || CollectionUtils.isEmpty(this.functionCall.getArguments())) {
            return null;
        }

        BaseExpression baseExpression = this.functionCall.getArguments().get(0);
        return baseExpression instanceof StringLiteral ? (StringLiteral)baseExpression : null;
    }

    public IntervalLiteral getIntervalLiteralArg() {
        if (this.functionCall == null || CollectionUtils.isEmpty(this.functionCall.getArguments())) {
            return null;
        }
        BaseExpression baseExpression = this.functionCall.getArguments().get(0);
        return baseExpression instanceof IntervalLiteral ? (IntervalLiteral)baseExpression : null;
    }

}
