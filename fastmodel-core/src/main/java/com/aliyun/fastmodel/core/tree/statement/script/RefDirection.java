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

package com.aliyun.fastmodel.core.tree.statement.script;

import lombok.Getter;

/**
 * RefRelationType
 *
 * @author panguanjing
 * @date 2021/9/14
 */
public enum RefDirection {
    /**
     * left -> right
     */
    LEFT_DIRECTION_RIGHT("->"),

    /**
     * right <- left
     */
    RIGHT_DIRECTON_LEFT("<-"),

    /**
     * left --> right
     */
    LEFT_VERTICAL_DIRECTION_RIGHT(">->"),

    /**
     * right --> left
     */
    RIGHT_VERTICAL_DIRECTON_LEFT("<-<"),
    ;

    @Getter
    private final String code;

    RefDirection(String code) {this.code = code;}

    public static RefDirection getByCode(String code) {
        for (RefDirection refRelationType : RefDirection.values()) {
            if (refRelationType.getCode().equalsIgnoreCase(code)) {
                return refRelationType;
            }
        }
        throw new IllegalArgumentException("can't find the code with:" + code);
    }
}
