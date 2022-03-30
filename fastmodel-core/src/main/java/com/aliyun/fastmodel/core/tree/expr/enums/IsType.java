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

package com.aliyun.fastmodel.core.tree.expr.enums;

/**
 * @author panguanjing
 * @date 2020/9/23
 */
public enum IsType {

    /**
     * null
     */
    NULL("NULL"),
    /**
     * true
     */
    TRUE("TRUE"),
    /**
     * false
     */
    FALSE("FALSE"),
    /**
     * not null
     */
    NOT_NULL("NOT NULL"),
    /**
     * not true
     */
    NOT_TRUE("NOT TRUE"),
    /**
     * not false
     */
    NOT_FALSE("NOT FALSE");

    private final String code;

    IsType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static IsType getByValue(String value) {
        IsType[] isTypes = IsType.values();
        for (IsType isType : isTypes) {
            if (isType.name().equalsIgnoreCase(value)) {
                return isType;
            }
        }
        throw new IllegalArgumentException("value can't mapping with:" + value);
    }
}
