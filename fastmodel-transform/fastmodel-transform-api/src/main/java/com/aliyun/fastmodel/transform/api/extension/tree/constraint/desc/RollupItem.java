package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * roll up item
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class RollupItem extends AbstractNode {

    private final Identifier rollupName;

    private final List<Identifier> columnList;

    private final List<Identifier> duplicateList;

    private final Identifier fromRollup;

    private final List<Property> properties;

    public RollupItem(Identifier rollupName, List<Identifier> columnList, List<Identifier> duplicateList, Identifier fromRollup,
        List<Property> properties) {
        this.rollupName = rollupName;
        this.columnList = columnList;
        this.duplicateList = duplicateList;
        this.fromRollup = fromRollup;
        this.properties = properties;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (rollupName != null) {
            builder.add(rollupName);
        }
        if (columnList != null) {
            builder.addAll(columnList);
        }
        if (duplicateList != null) {
            builder.addAll(duplicateList);
        }
        if (fromRollup != null) {
            builder.add(fromRollup);
        }
        if (properties != null) {
            builder.addAll(properties);
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitRollupItem(this, context);
    }
}
