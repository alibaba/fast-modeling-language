package com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;

/**
 * distribute key constraint
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class DistributeConstraint extends NonKeyConstraint {

    private final List<Identifier> columns;

    private final Boolean random;

    private final Integer bucket;

    public static final String TYPE = "DISTRIBUTE";

    public DistributeConstraint(List<Identifier> columns, Integer bucket) {
        this(columns, false, bucket);
    }

    public DistributeConstraint(List<Identifier> columns, Boolean random, Integer bucket) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.columns = columns;
        this.random = random;
        this.bucket = bucket;
    }

    public DistributeConstraint(boolean random, Integer bucket) {
        this(null, random, bucket);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksAstVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksAstVisitor.visitDistributeKeyConstraint(this, context);
    }
}
