package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * foreign key constraint
 *
 * @author panguanjing
 * @date 2024/2/18
 */
@Getter
public class ForeignKeyConstraint extends CustomConstraint {

    public enum MatchAction {
        SIMPLE,
        FULL,
        PARTIAL
    }

    public enum ReferenceAction {
        RESTRICT("RESTRICT"),
        CASCADE("CASCADE"),
        SET_NULL("SET NULL"),
        NO_ACTION("NO ACTION"),
        SET_DEFAULT("SET DEFAULT");
        @Getter
        private final String value;

        ReferenceAction(String value) {this.value = value;}
    }

    public enum ReferenceOperator {
        DELETE,
        UPDATE
    }

    private final Identifier indexName;
    private final List<Identifier> colNames;

    private final QualifiedName referenceTable;

    private final List<Identifier> referenceColNames;

    private final MatchAction matchAction;

    private final ReferenceOperator referenceOperator;

    private final ReferenceAction referenceAction;

    public ForeignKeyConstraint(Identifier constraintName, Identifier indexName, List<Identifier> colNames, QualifiedName referenceTable,
        List<Identifier> referenceColNames, MatchAction matchAction, ReferenceOperator referenceOperator, ReferenceAction referenceAction) {
        super(constraintName, true, "FOREIGN_KEY");
        this.indexName = indexName;
        this.colNames = colNames;
        this.referenceTable = referenceTable;
        this.referenceColNames = referenceColNames;
        this.matchAction = matchAction;
        this.referenceOperator = referenceOperator;
        this.referenceAction = referenceAction;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitForeignKeyConstraint(this, context);
    }
}
