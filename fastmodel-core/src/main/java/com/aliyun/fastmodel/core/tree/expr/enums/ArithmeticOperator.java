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
 * 数字运算
 *
 * @author panguanjing
 * @date 2020/11/5
 */
public enum ArithmeticOperator {
    /**
     * xor
     */
    XOR("^"),
    /**
     * star
     */
    STAR("*"),
    /**
     * divide
     */
    DIVIDE("/"),
    /**
     * %
     */
    MOD("%"),

    /**
     * +
     */
    ADD("+"),

    /**
     * -
     */
    SUBTRACT("-");

    private final String value;

    ArithmeticOperator(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ArithmeticOperator getByCode(String code) {
        ArithmeticOperator[] arithmeticOperators = ArithmeticOperator.values();
        for (ArithmeticOperator operator : arithmeticOperators) {
            if (operator.getValue().equalsIgnoreCase(code)) {
                return operator;
            }
        }
        throw new IllegalArgumentException("can't find the operator with code:" + code);
    }
}
