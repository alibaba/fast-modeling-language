package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.BaseSubPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionList;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * subhash template partition
 *
 * @author panguanjing
 * @date 2024/2/18
 */
@Getter
public class SubHashTemplatePartition extends BaseSubPartition {

    private final BaseExpression expression;

    private final SubPartitionList subPartitionList;

    public SubHashTemplatePartition(BaseExpression expression, SubPartitionList subPartitionList) {
        this.expression = expression;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitSubHashTemplatePartition(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (expression != null) {
            builder.add(expression);
        }
        if (subPartitionList != null) {
            builder.add(subPartitionList);
        }
        return builder.build();
    }
}
