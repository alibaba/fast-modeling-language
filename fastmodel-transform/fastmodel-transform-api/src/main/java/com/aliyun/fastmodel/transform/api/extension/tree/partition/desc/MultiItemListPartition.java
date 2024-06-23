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
public class MultiItemListPartition extends PartitionDesc {

    private final Identifier name;
    private final boolean ifNotExists;
    private final List<ListStringLiteral> listStringLiterals;
    private final List<Property> propertyList;

    public MultiItemListPartition(Identifier name, boolean ifNotExists,
        List<ListStringLiteral> listStringLiterals, List<Property> propertyList) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.listStringLiterals = listStringLiterals;
        this.propertyList = propertyList;
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
        if (listStringLiterals != null) {
            builder.addAll(listStringLiterals);
        }
        if (propertyList != null) {
            builder.addAll(propertyList);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitMultiItemListPartition(this, context);
    }
}
