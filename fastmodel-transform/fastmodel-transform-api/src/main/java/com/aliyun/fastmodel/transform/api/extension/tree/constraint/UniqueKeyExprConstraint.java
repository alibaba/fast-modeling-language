package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexSortKey;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * UniqueKeyExprConstraint
 *
 * @author panguanjing
 * @date 2024/2/19
 */
@Getter
public class UniqueKeyExprConstraint extends CustomConstraint {

    private final List<IndexSortKey> indexSortKeys;

    private final Algorithm algorithm;

    private final Identifier indexName;

    public UniqueKeyExprConstraint(Identifier constraintName, Identifier indexName, List<IndexSortKey> indexSortKeys, Algorithm algorithm) {
        super(constraintName, true, "UniqueKeyExpr");
        this.indexSortKeys = indexSortKeys;
        this.algorithm = algorithm;
        this.indexName = indexName;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitUniqueKeyExprConstraint(this, context);
    }
}
