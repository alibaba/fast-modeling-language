package com.aliyun.fastmodel.transform.api.extension.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * single item list
 *
 * @author panguanjing
 * @date 2023/10/17
 */
@Getter
public class SingleItemListPartition extends PartitionDesc {

    private final Identifier name;
    private final boolean ifNotExists;
    private final ListStringLiteral listStringLiteral;
    private final List<Property> propertyList;

    public SingleItemListPartition(Identifier name, boolean ifNotExists,
        ListStringLiteral listStringLiteral, List<Property> propertyList) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.listStringLiteral = listStringLiteral;
        this.propertyList = propertyList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitSingleItemListPartition(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (name != null) {
            builder.add(name);
        }
        if (name != null) {
            builder.add(name);
        }
        if (listStringLiteral != null) {
            builder.add(listStringLiteral);
        }
        if (propertyList != null) {
            builder.addAll(propertyList);
        }
        return builder.build();
    }
}
