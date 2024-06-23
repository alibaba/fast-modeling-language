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
 * BitOperator
 *
 * @author panguanjing
 * @date 2020/9/23
 */
public enum BitOperator {

    /**
     * CONCATENATE
     */
    CONCATENATE("||"),

    /**
     * AMPERSAND
     */
    AMPERSAND("&"),

    /**
     * BITWISE
     */
    BITWISE("|"),
    /**
     * !
     */
    NOT_MARK("!"),

    /**
     * <<
     */
    SHIFT_LEFT("<<"),

    /**
     * >>
     */
    SHIFT_RIGHT(">>"),

    /**
     * ^
     */
    BITWISE_XOR("^");

    /**
     * code
     */
    private final String code;

    private BitOperator(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static BitOperator getByCode(String code) {
        BitOperator[] operators = BitOperator.values();
        for (BitOperator e : operators) {
            if (e.getCode().equalsIgnoreCase(code)) {
                return e;
            }
        }
        throw new IllegalArgumentException("can't find exprOp with code:" + code);
    }

}
