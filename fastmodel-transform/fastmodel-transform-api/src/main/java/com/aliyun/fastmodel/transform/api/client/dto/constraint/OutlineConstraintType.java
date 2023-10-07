/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.constraint;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 默认的约束类型
 * https://www.w3schools.com/sql/sql_constraints.asp
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public enum OutlineConstraintType implements ConstraintType {
    /**
     * unique
     */
    UNIQUE(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.UNIQUE.getCode()),

    /**
     * Primary key
     */
    PRIMARY_KEY(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.PRIMARY_KEY.getCode()),

    /**
     * Check
     * CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
     */
    CHECK(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.CHECK.getCode()),

    /**
     * create index
     */
    CREATE_INDEX(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.INDEX.getCode());
    @Getter
    private final String value;

    OutlineConstraintType(String value) {
        this.value = value;
    }

    @Override
    public String getName() {
        return this.getValue();
    }

    /**
     * get constraint type by value
     *
     * @param value
     * @return
     */
    public static OutlineConstraintType getByValue(String value) {
        OutlineConstraintType[] outlineConstraintTypes = OutlineConstraintType.values();
        for (OutlineConstraintType o : outlineConstraintTypes) {
            if (StringUtils.equalsIgnoreCase(o.getValue(), value)) {
                return o;
            }
        }
        throw new IllegalArgumentException("can't find the outline constraint with:" + value);
    }
}
