package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * distribute key constraint
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class DistributeNonKeyConstraint extends NonKeyConstraint {

    private final List<Identifier> columns;

    private final Boolean random;

    private final Boolean replicated;

    private final Integer bucket;

    private final Boolean auto;

    public static final String TYPE = "DISTRIBUTE";

    public DistributeNonKeyConstraint(List<Identifier> columns, Boolean random, Boolean replicated, Integer bucket, Boolean auto) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.columns = columns;
        this.random = random;
        this.replicated = replicated;
        this.bucket = bucket;
        this.auto = auto;
    }

    public DistributeNonKeyConstraint(List<Identifier> columns, Boolean random, Boolean replicated, Integer bucket) {
        this(columns, random, replicated, bucket, null);
    }

    public DistributeNonKeyConstraint(List<Identifier> columns, Integer bucket) {
        this(columns, false, bucket);
    }

    public DistributeNonKeyConstraint(List<Identifier> columns, Boolean random, Integer bucket) {
        this(columns, random, false, bucket, false);
    }

    public DistributeNonKeyConstraint(boolean random, Integer bucket) {
        this(null, random, bucket);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitDistributeKeyConstraint(this, context);
    }
}
