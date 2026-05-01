/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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
@Getter
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
    CHECK(com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType.CHECK.getCode());

    private final String value;

    OutlineConstraintType(String value) {
        this.value = value;
    }

    @Override
    public String getCode() {
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
