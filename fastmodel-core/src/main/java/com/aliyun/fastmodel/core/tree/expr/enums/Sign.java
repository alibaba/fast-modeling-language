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
 *
 * @author panguanjing
 * @date 2020/9/23
 */
public enum Sign {
    /**
     * +
     */
    PLUS("+"),
    /**
     * -
     */
    MINUS("-"),
    /**
     *
     */
    TILDE("~"),

    /**
     * not
     */
    NOT("NOT");

    /**
     * code
     */
    private final String code;

    private Sign(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static Sign getByCode(String code) {
        Sign[] signs = Sign.values();
        for (Sign sign : signs) {
            if (sign.getCode().equalsIgnoreCase(code)) {
                return sign;
            }
        }
        throw new IllegalArgumentException("can't find the code with:" + code);
    }
}

