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

package com.aliyun.fastmodel.core.tree.statement.constants;

import lombok.Getter;

/**
 * 删除类型
 *
 * @author panguanjing
 * @date 2021/1/15
 */
public enum DeleteType {

    /**
     * 删除的表
     */
    TABLE(StatementType.TABLE),

    /**
     * 指标
     */
    INDICATOR(StatementType.INDICATOR),
    ;

    @Getter
    private final StatementType statementType;

    DeleteType(StatementType statementType) {
        this.statementType = statementType;
    }

    public static DeleteType getByCode(String code) {
        StatementType statementType = StatementType.getByCode(code);
        for (DeleteType deleteType : DeleteType.values()) {
            if (deleteType.getStatementType() == statementType) {
                return deleteType;
            }
        }
        throw new IllegalArgumentException("unsupported delete by code:" + code);
    }
}
