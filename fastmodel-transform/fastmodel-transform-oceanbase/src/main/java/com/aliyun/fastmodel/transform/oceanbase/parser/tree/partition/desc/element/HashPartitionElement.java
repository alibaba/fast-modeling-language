package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * hash partition element
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Getter
public class HashPartitionElement extends PartitionElement {

    private final QualifiedName qualifiedName;

    private final LongLiteral num;

    private final Property property;

    private final SubPartitionList subPartitionList;

    public HashPartitionElement(QualifiedName qualifiedName, LongLiteral num, Property property, SubPartitionList subPartitionList) {
        this.qualifiedName = qualifiedName;
        this.num = num;
        this.property = property;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (qualifiedName != null) {
            builder.add(qualifiedName);
        }
        if (num != null) {
            builder.add(num);
        }
        if (property != null) {
            builder.add(property);
        }
        if (subPartitionList != null) {
            builder.add(subPartitionList);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitHashPartitionElement(this, context);
    }
}
