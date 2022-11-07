/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.expr.literal;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * hexliteral
 *
 * @author panguanjing
 * @date 2022/7/10
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class HexLiteral extends BaseLiteral {

    private final String value;

    public HexLiteral(String value) {
        this(null, null, value);
    }

    public HexLiteral(NodeLocation location, String origin, String value) {
        super(location, origin);
        this.value = value;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitHexLiteral(this, context);
    }
}
