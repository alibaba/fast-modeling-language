package com.aliyun.fastmodel.core.tree.statement.show;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
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
public class ShowStatistic extends BaseQueryStatement {

    private final ShowObjectsType objectsType;

    public ShowStatistic(Identifier baseUnit, ShowObjectsType objectsType) {
        this(null, null, baseUnit, objectsType);
    }

    public ShowStatistic(NodeLocation nodeLocation, String origin, Identifier unit, ShowObjectsType showType) {
        super(nodeLocation, origin, unit);
        this.objectsType = showType;
        setStatementType(StatementType.SHOW_STATISTIC);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowStatistic(this, context);
    }

}
