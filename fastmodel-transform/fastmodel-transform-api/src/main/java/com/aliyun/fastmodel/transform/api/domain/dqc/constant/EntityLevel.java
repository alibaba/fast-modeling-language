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

package com.aliyun.fastmodel.transform.api.domain.dqc.constant;

import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import lombok.Getter;

/**
 * EntityLevel
 *
 * @author panguanjing
 * @date 2021/6/1
 */
public enum EntityLevel {

    /**
     * sql
     */
    SQL(RulesLevel.SQL, 0),

    /**
     * task
     */
    TASK(RulesLevel.TASK, 1);

    @Getter
    private RulesLevel level;

    @Getter
    private Integer value;

    EntityLevel(RulesLevel level, Integer value) {
        this.level = level;
        this.value = value;
    }

    public static EntityLevel getByRulesLevel(RulesLevel rulesLevel) {
        for (EntityLevel e : EntityLevel.values()) {
            if (e.level == rulesLevel) {
                return e;
            }
        }
        throw new IllegalArgumentException("can't find the entityLevel with:" + rulesLevel);
    }
}
