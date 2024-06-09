package com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import lombok.Getter;

/**
 * roll up constraint
 * ROLLUP (rollup_name (column_name1, column_name2, ...)
 * [FROM from_index_name]
 * [PROPERTIES ("key" = "value", ...)],...)
 *
 * @author panguanjing
 * @date 2023/12/15
 */
@Getter
public class RollupConstraint extends NonKeyConstraint {

    public static final String TYPE = "Rollup";

    private final List<RollupItem> rollupItemList;

    public RollupConstraint(List<RollupItem> rollupItemList) {
        super(IdentifierUtil.sysIdentifier(), true, TYPE);
        this.rollupItemList = rollupItemList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        StarRocksAstVisitor<R, C> starRocksAstVisitor = (StarRocksAstVisitor<R, C>)visitor;
        return starRocksAstVisitor.visitRollupConstraint(this, context);
    }
}
