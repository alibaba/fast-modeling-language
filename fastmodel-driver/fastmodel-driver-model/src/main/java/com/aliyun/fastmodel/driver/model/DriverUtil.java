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

package com.aliyun.fastmodel.driver.model;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/27
 */
public class DriverUtil {
    /**
     * 设置的select的语句的超时的词
     */
    private static final String[] SELECT_KEYWORDS = new String[] {"SELECT", "WITH", "SHOW", "DESC", "EXISTS",
        "CALL"};

    /**
     * 判断sql是否为select语句，使用一个比较简单的方式进行判断
     *
     * @param sql
     * @return
     */
    public static boolean isSelect(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            String nextTwo = sql.substring(i, Math.min(i + 2, sql.length()));
            if ("--".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("\n", i));
            } else if ("/*".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("*/", i));
            } else if (Character.isLetter(sql.charAt(i))) {
                String trimmed = sql.substring(i, Math.min(sql.length(), Math.max(i, sql.indexOf(" ", i))));
                for (String keyword : SELECT_KEYWORDS) {
                    if (trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

}
