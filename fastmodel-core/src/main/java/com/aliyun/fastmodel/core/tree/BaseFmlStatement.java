/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree;

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;
import lombok.Setter;

/**
 * 抽象的语句接口
 *
 * @author panguanjing
 * @date 2020/09/02
 */
@Getter
@Setter
public abstract class BaseFmlStatement extends BaseStatement {

    /**
     * 语句类型{@link StatementType}
     */
    private StatementType statementType;

    public BaseFmlStatement(NodeLocation nodeLocation) {
        this(nodeLocation, null);
    }

    public BaseFmlStatement(NodeLocation location, String origin) {
        super(location);
        this.origin = origin;
    }

    /**
     * abstract accept visitor
     *
     * @param visitor
     * @param context
     * @param <R>
     * @param <C>
     * @return
     */
    public abstract <R, C> R accept(AstVisitor<R, C> visitor, C context);

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return (R)accept((AstVisitor)visitor, context);
    }

    @Override
    public String toString() {
        try {
            return FastModelFormatter.formatNode(this);
        } catch (Exception ignore) {
            return getClass() + ":{statementType:" + statementType + ";origin:" + origin + "}";
        }
    }

}
