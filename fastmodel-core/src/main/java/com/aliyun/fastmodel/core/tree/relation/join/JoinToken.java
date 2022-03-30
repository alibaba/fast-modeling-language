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

package com.aliyun.fastmodel.core.tree.relation.join;

/**
 * join Token
 *
 * @author panguanjing
 * @date 2020/10/20
 */
public enum JoinToken {

    /**
     * inner join
     */
    INNER("INNER"),
    /**
     * left join
     */
    LEFT("LEFT"),
    /**
     * right join
     */
    RIGHT("RIGHT"),
    /**
     * full join
     */
    FULL("FULL"),

    CROSS("CROSS"),

    /**
     * left semi join
     */
    IMPLICIT("IMPLICIT");

    private final String code;

    private JoinToken(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static JoinToken getByCode(String code) {
        JoinToken[] joinTokens = JoinToken.values();
        for (JoinToken joinToken : joinTokens) {
            if (joinToken.getCode().equalsIgnoreCase(code)) {
                return joinToken;
            }
        }
        throw new IllegalArgumentException("can't find the joinToken with code:" + code);
    }
}
