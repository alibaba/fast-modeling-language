package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Data;

/**
 * hash sub partition element
 *
 * @author panguanjing
 * @date 2024/2/7
 */
@Data
public class SubHashPartitionElement extends SubPartitionElement {
    private final QualifiedName qualifiedName;

    private final Property property;

    public SubHashPartitionElement(QualifiedName qualifiedName, Property property) {
        this.qualifiedName = qualifiedName;
        this.property = property;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (qualifiedName != null) {
            builder.add(qualifiedName);
        }
        if (property != null) {
            builder.add(property);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitHashSubPartitionElement(this, context);
    }
}
