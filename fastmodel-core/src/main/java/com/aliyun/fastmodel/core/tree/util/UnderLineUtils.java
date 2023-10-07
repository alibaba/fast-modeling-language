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

package com.aliyun.fastmodel.core.tree.util;

import java.util.Locale;

import org.apache.commons.lang3.StringUtils;

/**
 * UnderLineUtils
 *
 * @author panguanjing
 * @date 2021/3/17
 */
public class UnderLineUtils {

    private final static String UNDERLINE = "_";

    public static String underlineToHump(String keyword) {
        if (StringUtils.isBlank(keyword)) {
            return keyword;
        }
        String[] k = keyword.split(UNDERLINE);
        if (k.length == 1) {
            return keyword;
        } else {
            String f = StringUtils.uncapitalize(k[0]);
            StringBuilder stringBuilder = new StringBuilder(f);
            for (int i = 1; i < k.length; i++) {
                stringBuilder.append(StringUtils.capitalize(k[i]));
            }
            return stringBuilder.toString();
        }
    }

    public static String humpToUnderLine(String keyword) {
        if (StringUtils.isBlank(keyword)) {
            return keyword;
        }
        StringBuilder builder = new StringBuilder(keyword);
        int temp = 0;
        if (keyword.indexOf(UNDERLINE) < 0) {
            for (int i = 0; i < keyword.length(); i++) {
                if (Character.isUpperCase(keyword.charAt(i))) {
                    builder.insert(i + temp, UNDERLINE);
                    temp += 1;
                }
            }
        }
        return builder.toString().toLowerCase(Locale.ROOT);
    }
}
