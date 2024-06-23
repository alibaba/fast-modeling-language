package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * sub hash partition
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubHashPartition extends BaseSubPartition {

    private final BaseExpression expression;
    private final LongLiteral subpartitionCount;

    public SubHashPartition(BaseExpression expression, LongLiteral subpartitionCount) {
        Preconditions.checkNotNull(expression, "expression can't be null");
        this.expression = expression;
        this.subpartitionCount = subpartitionCount;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression, subpartitionCount);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> extensionAstVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubHashPartition(this, context);
    }
}
