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

import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/1
 */
public enum CheckerType {
    /**
     *
     */
    FIX_STRATEGY_CHECK("fix_strategy_check", 9),
    VOL_STRATEGY_CHECK("vol_strategy_check", 6),
    DYNAMIC_STRATEGY_CHECK("dynamic_strategy_check", 12);

    @Getter
    private String name;
    @Getter
    private Integer type;

    CheckerType(String name, Integer type) {
        this.name = name;
        this.type = type;
    }
}
