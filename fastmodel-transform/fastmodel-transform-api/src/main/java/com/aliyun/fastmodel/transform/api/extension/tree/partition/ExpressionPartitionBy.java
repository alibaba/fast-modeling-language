package com.aliyun.fastmodel.transform.api.extension.tree.partition;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

/**
 * ExpressionPartitionedBy
 *
 * @author 子梁
 * @author panguanjing 为支持扩展移动到api
 * @date 2023/12/26
 */
@Getter
public class ExpressionPartitionBy extends PartitionedBy {

    /**
     * 函数表达式
     */
    private final FunctionCall functionCall;

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
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitExpressionPartitionedBy(this, context);
    }

    public TableOrColumn getColumn(Integer argIndex) {
        if (this.functionCall == null || CollectionUtils.isEmpty(this.functionCall.getArguments())) {
            return null;
        }

        BaseExpression baseExpression = this.functionCall.getArguments().get(argIndex);
        return baseExpression instanceof TableOrColumn ? (TableOrColumn)baseExpression : null;
    }

    public StringLiteral getTimeUnitArg(Integer argIndex) {
        if (this.functionCall == null || CollectionUtils.isEmpty(this.functionCall.getArguments())) {
            return null;
        }

        BaseExpression baseExpression = this.functionCall.getArguments().get(argIndex);
        return baseExpression instanceof StringLiteral ? (StringLiteral)baseExpression : null;
    }

    public IntervalLiteral getIntervalLiteralArg(Integer argIndex) {
        if (this.functionCall == null || CollectionUtils.isEmpty(this.functionCall.getArguments())) {
            return null;
        }
        BaseExpression baseExpression = this.functionCall.getArguments().get(argIndex);
        return baseExpression instanceof IntervalLiteral ? (IntervalLiteral)baseExpression : null;
    }

    @Override
    public boolean isNotEmpty() {
        return super.isNotEmpty() || functionCall != null;
    }
}
