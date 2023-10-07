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

package com.aliyun.fastmodel.core.tree.statement.group;

import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/8
 */
public enum GroupType {
    /**
     * 度量单位
     */
    MEASURE_UNIT(StatementType.MEASURE_UNIT),

    /**
     * 数据字典
     */
    DICT(StatementType.DICT),

    /**
     * 标准代码
     */
    CODE(StatementType.TABLE);

    @Getter
    private final StatementType statementType;

    private GroupType(StatementType statementType) {
        this.statementType = statementType;
    }

    public static GroupType getByCode(String code) {
        GroupType[] groupTypes = GroupType.values();
        for (GroupType t : groupTypes) {
            if (t.name().equalsIgnoreCase(code)) {
                return t;
            }
        }
        throw new IllegalArgumentException("can't find the groupType with code:" + code);
    }
}
