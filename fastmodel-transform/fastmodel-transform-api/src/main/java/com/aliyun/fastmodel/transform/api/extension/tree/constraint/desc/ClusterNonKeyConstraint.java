package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * cluster by constraint
 *
 * @author panguanjing
 * @date 2024/1/21
 */
@Getter
public class ClusterNonKeyConstraint extends NonKeyConstraint {

    public static final String TYPE = "Cluster";

    private final List<Identifier> columns;

    public ClusterNonKeyConstraint(Identifier constraintName, Boolean enable, List<Identifier> columns) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public ClusterNonKeyConstraint(List<Identifier> columns) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.columns = columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitClusterKeyConstraint(this, context);
    }
}
