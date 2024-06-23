/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.expr.literal;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import lombok.Getter;

/**
 * escape string literal
 *
 * @author panguanjing
 * @date 2022/6/10
 */
@Getter
public class EscapeStringLiteral extends StringLiteral {
    /**
     * escape value
     */
    private final String escapeValue;

    public EscapeStringLiteral(String value, String escapeValue) {
        super(value);
        this.escapeValue = escapeValue;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitEscapeStringLiteral(this, context);
    }
}
