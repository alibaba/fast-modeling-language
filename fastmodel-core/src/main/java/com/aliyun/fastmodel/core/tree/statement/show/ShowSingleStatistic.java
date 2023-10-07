package com.aliyun.fastmodel.core.tree.statement.show;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * show statistic
 *
 * @author panguanjing
 * @date 2022/12/18
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class ShowSingleStatistic extends BaseQueryStatement {

    private final ShowType showType;

    private final QualifiedName qualifiedName;

    public ShowSingleStatistic(Identifier unit, ShowType showType, QualifiedName qualifiedName) {
        this(null, null, unit, showType, qualifiedName);
    }

    public ShowSingleStatistic(NodeLocation nodeLocation,
        String origin, Identifier unit, ShowType showType, QualifiedName qualifiedName
    ) {
        super(nodeLocation, origin, unit);
        this.showType = showType;
        Preconditions.checkNotNull(showType, "show Type can't be null");
        Preconditions.checkNotNull(qualifiedName, "qualifiedName can't be null");
        this.qualifiedName = qualifiedName;
        setStatementType(StatementType.SHOW_SINGLE_STATISTIC);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSingleStatistic(this, context);
    }

}
